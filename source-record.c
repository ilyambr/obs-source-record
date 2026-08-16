#include <obs-module.h>
#include <obs-frontend-api.h>
#include <util/config-file.h>
#include <util/platform.h>
#include <util/threading.h>
#include <util/dstr.h>
#include "version.h"
#include "obs-websocket-api.h"

#ifndef _WIN32
#include <dlfcn.h>
#endif

#define OUTPUT_MODE_NONE 0
#define OUTPUT_MODE_ALWAYS 1
#define OUTPUT_MODE_STREAMING 2
#define OUTPUT_MODE_RECORDING 3
#define OUTPUT_MODE_STREAMING_OR_RECORDING 4
#define OUTPUT_MODE_VIRTUAL_CAMERA 5

#define AUDIO_TRACK_CUSTOM -2

#define BACKGROUND_CHANNEL 0
#define SOURCE_CHANNEL 1
#define OVERLAY_CHANNEL 2
#define MAX_OVERLAY_SOURCES 4

struct source_record_filter_context {
	obs_source_t *source;
	video_t *video_output;
	audio_t *audio_output;
	bool output_active;
	uint32_t width;
	uint32_t height;
	uint64_t last_frame_time_ns;
	obs_view_t *view;
	obs_scene_t *overlay_scene;
	bool starting_file_output;
	bool starting_stream_output;
	bool starting_replay_output;
	bool restart;
	obs_output_t *fileOutput;
	obs_output_t *streamOutput;
	obs_output_t *replayOutput;
	obs_encoder_t *encoder;
	obs_encoder_t *audioEncoder[MAX_AUDIO_MIXES];
	obs_service_t *service;
	bool record;
	bool stream;
	bool replayBuffer;
	obs_hotkey_pair_id enableHotkey;
	obs_hotkey_pair_id pauseHotkeys;
	obs_hotkey_pair_id recordHotkeys;
	obs_hotkey_id splitHotkey;
	obs_hotkey_id chapterHotkey;
	int audio_track;
	uint32_t audio_track_mask;
	/* audio_source itself (the pointer stored in the field below) is written
	 * from source_record_filter_update, which can run on the UI thread or
	 * (via the websocket vendor API) an arbitrary caller thread, while
	 * audio_input_callback reads/dereferences it concurrently on OBS's audio
	 * mixer thread -- with no lock, changing the "Different Audio Source"
	 * setting while an output is active could free the weak-source wrapper
	 * on one thread at the same instant the audio thread is mid-call into
	 * obs_weak_source_get_source() on it. audio_source_mutex guards every
	 * read and write of this field. */
	pthread_mutex_t audio_source_mutex;
	obs_weak_source_t *audio_source;
	bool closing;
	bool exiting;
	long long replay_buffer_duration;
	bool replay_error;
	bool record_error;
	/* The exact resolved file path (timestamp placeholders already expanded)
	 * the most recent start_file_output() computed and handed to the output
	 * as its own "path" setting -- kept around so get_record_status can
	 * report it, same idea as ffmpeg-mux's own get_last_replay proc handler,
	 * which only exists for the replay-buffer output variant, not this one. */
	struct dstr last_output_path;
	struct vec4 backgroundColor;
	bool remove_after_record;
	long long record_max_seconds;
	int last_frontend_event;
	bool source_hidden_in_scene;      // last known hidden state, to detect transitions
	bool record_mode_hidden_override; // true if we forced record_mode to None because of hiding
	long long saved_record_mode;      // record_mode to restore once un-hidden
	float visibility_check_accum;     // throttle: only check every ~0.5s, not every tick

	// Throttle for check_encoder_overload: only check every ~2s, not every
	// tick. prev_total/prev_dropped per output are baselines so the rate
	// reported is "since the last check", not a lifetime-cumulative fraction
	// that would stay diluted-looking long after a transient problem
	// cleared up. Tracked per-filter-instance even for the 3 main-program
	// outputs (not shared/global) -- simpler than synchronizing a shared
	// baseline across every filter instance's own tick callback, at the
	// minor cost of each instance independently recomputing the same delta.
	float encoder_overload_check_accum;
	int own_prev_total, own_prev_dropped;
	int main_rec_prev_total, main_rec_prev_dropped;
	int main_stream_prev_total, main_stream_prev_dropped;
	int main_replay_prev_total, main_replay_prev_dropped;
};

DARRAY(obs_source_t *) source_record_filters;

static void *vendor;

static void run_queued(obs_task_t task, void *param)
{
	if (obs_in_task_thread(OBS_TASK_UI) && obs_get_video()) {
		obs_queue_task(OBS_TASK_GRAPHICS, task, param, false);
	} else {
		obs_queue_task(OBS_TASK_UI, task, param, false);
	}
}

/* obs_output_release() (-> obs_output_destroy()) is what actually frees an
 * output's buffered memory. For a replay_buffer output that can mean walking
 * and freeing up to max_size_mb (we hardcode 10000, i.e. ~10GB) of buffered
 * packets -- dispatching that through run_queued() above put it on
 * OBS_TASK_GRAPHICS/OBS_TASK_UI, both threads the rest of OBS's rendering and
 * UI depend on staying responsive, so freeing a large buffer there froze the
 * entire OBS window for as long as the free took (reported: ~30s, with memory
 * visibly draining during the freeze). libobs has a dedicated OBS_TASK_DESTROY
 * background thread (obs->destruction_task_thread) for exactly this kind of
 * expensive teardown work -- upstream's own mp4-output.c offloads its muxer
 * destruction the same way -- so route the actual release there instead. */
static void run_destroy_queued(obs_task_t task, void *param)
{
	obs_queue_task(OBS_TASK_DESTROY, task, param, false);
}

static void noop_task(void *param)
{
	UNUSED_PARAMETER(param);
}

/* release_encoders (queued above via run_destroy_queued from
 * release_output_stopped/force_stop_output_task) takes `context` itself as
 * its param, not a raw handle like obs_output_release's calls do -- it needs
 * to re-check context->record/stream/replayBuffer at the moment it actually
 * runs, not when it was queued, specifically so a quick re-show/re-enable
 * that reactivates the SAME encoder in between doesn't get it yanked out
 * from under the new activity (see release_encoders' own comment). That
 * live re-check is exactly why it can't just take a raw obs_encoder_t handle
 * like the output releases do.
 *
 * But OBS_TASK_DESTROY is a genuinely separate background thread with no
 * synchronization against filter_destroy freeing `context` out from under
 * it -- source_record_filter_destroy can run (and bfree(context)) while an
 * earlier stop's release_encoders(context) task is still sitting in that
 * queue, a real cross-thread use-after-free (caught by review, not found
 * live). Call this right before bfree(context): it queues a genuine no-op
 * onto the SAME queue and blocks until it's actually run. OBS_TASK_DESTROY
 * is FIFO, so by the time this returns, everything queued to it earlier --
 * including any release_encoders(context) for THIS context -- has already
 * finished, making the free that follows safe. Deliberately only called
 * from filter_destroy (a rare, deliberate action -- removing the filter, or
 * OBS closing), not from the routine hide/show path: blocking there would
 * reintroduce the exact render-thread stall this whole run_destroy_queued
 * mechanism exists to avoid for the common case.
 *
 * Guarded against ever calling this FROM OBS_TASK_DESTROY itself (self-
 * deadlock, waiting on a queue's own worker thread from inside that same
 * thread) -- not just defensive: if filter_destroy is somehow already
 * running as a task ON that queue, the FIFO ordering guarantee this
 * function exists to provide is already satisfied for free (nothing queued
 * before it could still be pending), so skipping the wait there is both
 * safe and correct, not merely deadlock-avoidance. */
static void wait_for_destroy_queue_drain(void)
{
	if (obs_in_task_thread(OBS_TASK_DESTROY))
		return;
	obs_queue_task(OBS_TASK_DESTROY, noop_task, NULL, true);
}

static const char *source_record_filter_get_name(void *unused)
{
	UNUSED_PARAMETER(unused);
	return "Source Record";
}

struct video_frame {
	uint8_t *data[MAX_AV_PLANES];
	uint32_t linesize[MAX_AV_PLANES];
};

static bool EncoderAvailable(const char *encoder)
{
	const char *val;
	int i = 0;

	while (obs_enum_encoder_types(i++, &val))
		if (strcmp(val, encoder) == 0)
			return true;

	return false;
}

static void calc_min_ts(obs_source_t *parent, obs_source_t *child, void *param)
{
	UNUSED_PARAMETER(parent);
	uint64_t *min_ts = param;
	if (!child || obs_source_audio_pending(child))
		return;
	const uint64_t ts = obs_source_get_audio_timestamp(child);
	if (!ts)
		return;
	if (!*min_ts || ts < *min_ts)
		*min_ts = ts;
}

static void mix_audio(obs_source_t *parent, obs_source_t *child, void *param)
{
	UNUSED_PARAMETER(parent);
	if (!child || obs_source_audio_pending(child))
		return;
	const uint64_t ts = obs_source_get_audio_timestamp(child);
	if (!ts)
		return;
	struct obs_source_audio *mixed_audio = param;
	const size_t pos = (size_t)ns_to_audio_frames(mixed_audio->samples_per_sec, ts - mixed_audio->timestamp);

	if (pos > AUDIO_OUTPUT_FRAMES)
		return;

	const size_t count = AUDIO_OUTPUT_FRAMES - pos;

	struct obs_source_audio_mix child_audio;
	obs_source_get_audio_mix(child, &child_audio);
	for (size_t ch = 0; ch < (size_t)mixed_audio->speakers; ch++) {
		float *out = ((float *)mixed_audio->data[ch]) + pos;
		float *in = child_audio.output[0].data[ch];
		if (!in)
			continue;
		for (size_t i = 0; i < count; i++) {
			out[i] += in[i];
		}
	}
}

static bool audio_input_callback(void *param, uint64_t start_ts_in, uint64_t end_ts_in, uint64_t *out_ts, uint32_t mixers,
				 struct audio_output_data *mixes)
{
	UNUSED_PARAMETER(end_ts_in);
	struct source_record_filter_context *filter = param;
	if (filter->closing || obs_source_removed(filter->source)) {
		*out_ts = start_ts_in;
		return true;
	}

	obs_source_t *audio_source = NULL;
	bool release_audio = false;
	/* Locked so filter->audio_source can't be released/replaced by
	 * source_record_filter_update on another thread between checking it's
	 * non-NULL and actually resolving it -- see the field's own comment. */
	pthread_mutex_lock(&filter->audio_source_mutex);
	bool had_weak_ref = filter->audio_source != NULL;
	if (had_weak_ref) {
		audio_source = obs_weak_source_get_source(filter->audio_source);
		if (audio_source)
			release_audio = true;
	}
	pthread_mutex_unlock(&filter->audio_source_mutex);
	if (!had_weak_ref) {
		audio_source = obs_filter_get_parent(filter->source);
	}
	if (!audio_source || obs_source_removed(audio_source)) {
		*out_ts = start_ts_in;
		if (release_audio)
			obs_source_release(audio_source);
		return true;
	}

	const uint32_t flags = obs_source_get_output_flags(audio_source);
	if ((flags & OBS_SOURCE_COMPOSITE) != 0) {
		uint64_t min_ts = 0;
		obs_source_enum_active_tree(audio_source, calc_min_ts, &min_ts);
		if (min_ts) {
			struct obs_source_audio mixed_audio = {0};
			for (size_t i = 0; i < MAX_AUDIO_CHANNELS; i++) {
				mixed_audio.data[i] = (uint8_t *)mixes->data[i];
			}
			mixed_audio.timestamp = min_ts;
			mixed_audio.speakers = audio_output_get_channels(filter->audio_output);
			mixed_audio.samples_per_sec = audio_output_get_sample_rate(filter->audio_output);
			mixed_audio.format = AUDIO_FORMAT_FLOAT_PLANAR;
			obs_source_enum_active_tree(audio_source, mix_audio, &mixed_audio);

			for (size_t mix_idx = 0; mix_idx < MAX_AUDIO_MIXES; mix_idx++) {
				if ((mixers & (1 << mix_idx)) == 0)
					continue;
				// clamp audio
				for (size_t ch = 0; ch < (size_t)mixed_audio.speakers; ch++) {
					float *mix_data = mixes[mix_idx].data[ch];
					float *mix_end = &mix_data[AUDIO_OUTPUT_FRAMES];

					while (mix_data < mix_end) {
						float val = *mix_data;
						val = (val > 1.0f) ? 1.0f : val;
						val = (val < -1.0f) ? -1.0f : val;
						*(mix_data++) = val;
					}
				}
			}
			*out_ts = min_ts;
		} else {
			*out_ts = start_ts_in;
		}
		if (release_audio)
			obs_source_release(audio_source);
		return true;
	}
	if ((flags & OBS_SOURCE_AUDIO) == 0) {
		*out_ts = start_ts_in;
		if (release_audio)
			obs_source_release(audio_source);
		return true;
	}

	const uint64_t source_ts = obs_source_get_audio_timestamp(audio_source);
	if (!source_ts) {
		*out_ts = start_ts_in;
		if (release_audio)
			obs_source_release(audio_source);
		return true;
	}

	if (obs_source_audio_pending(audio_source)) {
		if (release_audio)
			obs_source_release(audio_source);
		return false;
	}

	struct obs_source_audio_mix audio;
	obs_source_get_audio_mix(audio_source, &audio);

	const size_t channels = audio_output_get_channels(filter->audio_output);
	for (size_t mix_idx = 0; mix_idx < MAX_AUDIO_MIXES; mix_idx++) {
		if ((mixers & (1 << mix_idx)) == 0)
			continue;
		for (size_t ch = 0; ch < channels; ch++) {
			float *out = mixes[mix_idx].data[ch];
			float *in = audio.output[0].data[ch];
			if (!in)
				continue;
			for (size_t i = 0; i < AUDIO_OUTPUT_FRAMES; i++) {
				out[i] += in[i];
				if (out[i] > 1.0f)
					out[i] = 1.0f;
				if (out[i] < -1.0f)
					out[i] = -1.0f;
			}
		}
	}

	*out_ts = source_ts;
	if (release_audio)
		obs_source_release(audio_source);

	return true;
}

static const char *GetFormatExt(const char *format)
{
	if (strcmp(format, "fragmented_mp4") == 0)
		return "mp4";
	if (strcmp(format, "hybrid_mp4") == 0)
		return "mp4";
	if (strcmp(format, "hybrid_mov") == 0)
		return "mov";
	if (strcmp(format, "fragmented_mov") == 0)
		return "mov";
	if (strcmp(format, "hls") == 0)
		return "m3u8";
	if (strcmp(format, "mpegts") == 0)
		return "ts";
	return format;
}

static void start_file_output_task(void *data)
{
	struct source_record_filter_context *context = data;
	if (obs_output_start(context->fileOutput)) {
		if (!context->output_active) {
			context->output_active = true;
			obs_source_inc_showing(obs_filter_get_parent(context->source));
		}
	}
	context->starting_file_output = false;
}

static void start_stream_output_task(void *data)
{
	struct source_record_filter_context *context = data;
	if (obs_output_start(context->streamOutput)) {
		if (!context->output_active) {
			context->output_active = true;
			obs_source_inc_showing(obs_filter_get_parent(context->source));
		}
	}
	context->starting_stream_output = false;
}

static void release_encoders(void *param)
{
	struct source_record_filter_context *context = param;
	if (context->source && obs_source_enabled(context->source) && (context->replayBuffer || context->record || context->stream))
		return;
	if (context->encoder && !obs_encoder_active(context->encoder)) {
		obs_encoder_release(context->encoder);
		context->encoder = NULL;
	}
	for (int i = 0; i < MAX_AUDIO_MIXES; i++) {
		if (!context->audioEncoder[i] || obs_encoder_active(context->audioEncoder[i]))
			continue;
		obs_encoder_release(context->audioEncoder[i]);
		context->audioEncoder[i] = NULL;
	}
}

struct stop_output {
	struct source_record_filter_context *context;
	obs_output_t *output;
};

static void source_record_replay_saved(void *data, calldata_t *cd)
{
	struct source_record_filter_context *context = data;
	if (!context || !context->source)
		return;

	/* "saved" only ever fires on a genuine success (a failed/interrupted
	 * flush instead fires "stop" with a non-success code, handled below in
	 * source_record_replay_stopped) -- but that stop handler is the only
	 * place replay_error gets set, and nothing previously cleared it back
	 * to false afterward, so a single failed save (e.g. writing to a RAM
	 * disk that briefly wasn't mounted) left the row stuck red/"Error"
	 * indefinitely, even once saves started succeeding again. */
	context->replay_error = false;

	const char *path = calldata_string(cd, "path");
	char *path_fallback = NULL;
	if ((!path || !strlen(path)) && context->replayOutput) {
		proc_handler_t *ph = obs_output_get_proc_handler(context->replayOutput);
		if (ph) {
			calldata_t proc_cd;
			calldata_init(&proc_cd);
			if (proc_handler_call(ph, "get_last_replay", &proc_cd)) {
				const char *last_path = calldata_string(&proc_cd, "path");
				if (last_path && strlen(last_path))
					path_fallback = bstrdup(last_path);
			}
			calldata_free(&proc_cd);
		}
	}

	const char *emit_path = "";
	if (path && strlen(path)) {
		emit_path = path;
	} else if (path_fallback) {
		emit_path = path_fallback;
	}

	obs_data_t *event_data = obs_data_create();
	obs_data_set_string(event_data, "path", emit_path);
	obs_data_set_string(event_data, "filter", obs_source_get_name(context->source));

	obs_source_t *parent = obs_filter_get_parent(context->source);
	if (parent) {
		obs_data_set_string(event_data, "source", obs_source_get_name(parent));
	}

	obs_websocket_vendor_emit_event(vendor, "replay_buffer_saved", event_data);
	obs_data_release(event_data);

	/* Plain (non-websocket-dependent) signal for other plugins to hook. */
	signal_handler_t *filter_sh = obs_source_get_signal_handler(context->source);
	if (filter_sh) {
		calldata_t signal_cd;
		calldata_init(&signal_cd);
		calldata_set_string(&signal_cd, "path", emit_path);
		signal_handler_signal(filter_sh, "replay_saved", &signal_cd);
		calldata_free(&signal_cd);
	}

	bfree(path_fallback);
}

static void source_record_replay_stopped(void *data, calldata_t *cd)
{
	struct source_record_filter_context *context = data;
	const long long code = calldata_int(cd, "code");
	context->replay_error = code != OBS_OUTPUT_SUCCESS;
}

/* Mirrors source_record_replay_stopped above -- fills in the "no equivalent
 * error flag tracked for record" gap get_record_status_proc's own comment
 * used to call out. Connected unconditionally (not just when
 * remove_after_record is set) so a row's Error status is accurate regardless
 * of that option. */
static void source_record_file_stopped(void *data, calldata_t *cd)
{
	struct source_record_filter_context *context = data;
	const long long code = calldata_int(cd, "code");
	context->record_error = code != OBS_OUTPUT_SUCCESS;
}

struct find_output_hotkey_ctx {
	/* obs_hotkey_register_output() stores the registerer as the output's weak
	 * wrapper (obs_output_get_weak_output()), not the raw obs_output_t*, so
	 * that's what has to be compared against here. */
	obs_weak_output_t *target_weak;
	obs_hotkey_id id;
};

static bool find_output_hotkey_cb(void *data, obs_hotkey_id id, obs_hotkey_t *key)
{
	struct find_output_hotkey_ctx *ctx = data;
	if (obs_hotkey_get_registerer_type(key) != OBS_HOTKEY_REGISTERER_OUTPUT)
		return true;
	if (obs_hotkey_get_registerer(key) != (void *)ctx->target_weak)
		return true;
	ctx->id = id;
	return false;
}

struct find_hotkey_binding_ctx {
	obs_hotkey_id id;
	obs_key_combination_t combo;
	bool found;
};

static bool find_hotkey_binding_cb(void *data, size_t idx, obs_hotkey_binding_t *binding)
{
	UNUSED_PARAMETER(idx);
	struct find_hotkey_binding_ctx *ctx = data;
	if (obs_hotkey_binding_get_hotkey_id(binding) != ctx->id)
		return true;
	ctx->combo = obs_hotkey_binding_get_key_combination(binding);
	ctx->found = true;
	return false;
}

struct find_named_source_hotkey_ctx {
	/* Several distinct hotkeys (enable/disable, pause/unpause, split,
	 * chapter, start/stop record, ...) all register against the same
	 * parent source, so registerer alone isn't enough to pick one out --
	 * this also matches on the hotkey's own registration name. */
	obs_weak_source_t *target_weak;
	const char *name;
	obs_hotkey_id id;
};

static bool find_named_source_hotkey_cb(void *data, obs_hotkey_id id, obs_hotkey_t *key)
{
	struct find_named_source_hotkey_ctx *ctx = data;
	if (obs_hotkey_get_registerer_type(key) != OBS_HOTKEY_REGISTERER_SOURCE)
		return true;
	if (obs_hotkey_get_registerer(key) != (void *)ctx->target_weak)
		return true;
	if (strcmp(obs_hotkey_get_name(key), ctx->name) != 0)
		return true;
	ctx->id = id;
	return false;
}

/* Same idea as get_output_hotkey_str below, but for a single hotkey
 * registered on `parent` (obs_hotkey_register_source /
 * obs_hotkey_pair_register_source) and identified by its registration name
 * -- e.g. "source_record.StartRecording" -- rather than by registerer type
 * alone. Leaves `str` empty if the hotkey isn't found or has no binding. */
static void get_source_hotkey_str(obs_source_t *parent, const char *hotkey_name, struct dstr *str)
{
	dstr_free(str);
	if (!parent)
		return;

	obs_weak_source_t *weak_parent = obs_source_get_weak_source(parent);
	struct find_named_source_hotkey_ctx find_ctx = {weak_parent, hotkey_name, OBS_INVALID_HOTKEY_ID};
	obs_enum_hotkeys(find_named_source_hotkey_cb, &find_ctx);
	if (weak_parent)
		obs_weak_source_release(weak_parent);
	if (find_ctx.id == OBS_INVALID_HOTKEY_ID)
		return;

	struct find_hotkey_binding_ctx bind_ctx = {find_ctx.id, {0}, false};
	obs_enum_hotkey_bindings(find_hotkey_binding_cb, &bind_ctx);
	if (bind_ctx.found)
		obs_key_combination_to_str(bind_ctx.combo, str);
}

/* Finds the (single) hotkey the "replay_buffer" output type registers for
 * itself and writes its bound key combination (e.g. "F8") into `str`, or
 * leaves `str` empty if the output has no hotkey or it is unbound. */
static void get_output_hotkey_str(obs_output_t *output, struct dstr *str)
{
	dstr_free(str);
	if (!output)
		return;

	obs_weak_output_t *weak_output = obs_output_get_weak_output(output);
	struct find_output_hotkey_ctx find_ctx = {weak_output, OBS_INVALID_HOTKEY_ID};
	obs_enum_hotkeys(find_output_hotkey_cb, &find_ctx);
	if (weak_output)
		obs_weak_output_release(weak_output);
	if (find_ctx.id == OBS_INVALID_HOTKEY_ID)
		return;

	struct find_hotkey_binding_ctx bind_ctx = {find_ctx.id, {0}, false};
	obs_enum_hotkey_bindings(find_hotkey_binding_cb, &bind_ctx);
	if (bind_ctx.found)
		obs_key_combination_to_str(bind_ctx.combo, str);
}

/* Mirrors get_replay_buffer_status_proc below, but for the filter's own file
 * recording: reports whether record_mode currently resolves to on, whether
 * the file output is actually running, whether the last stop was a failure
 * rather than a normal one (mirrors replay_error), the exact resolved path
 * of the most recent (or current) recording -- same idea as ffmpeg-mux's own
 * get_last_replay proc, which only exists for the replay-buffer output
 * variant, not this one -- and the bound key combination (if any) for the
 * "source_record.StartRecording" hotkey registered below, same shape as
 * get_replay_buffer_status_proc's own "hotkey" field. Lets external docks
 * (e.g. obs-replay-slider's control panel) show live status without
 * duplicating this filter's own record_mode bookkeeping. */
static void get_record_status_proc(void *data, calldata_t *cd)
{
	struct source_record_filter_context *context = data;
	calldata_set_bool(cd, "enabled", context->record);
	calldata_set_bool(cd, "active", context->fileOutput && obs_output_active(context->fileOutput));
	calldata_set_bool(cd, "error", context->record_error);
	/* source_hidden_in_scene is update_hidden_record_mode's own tracked state
	 * (see its own comment) -- ticks every 0.5s off video_tick regardless of
	 * whether this source is showing anywhere, so it's always fresh here.
	 * Exposed so an external dock (obs-replay-slider) can distinguish "hidden
	 * in its scene" from "just not recording" without re-deriving the same
	 * per-scene-item-visibility check itself (a naive obs_source_active()
	 * check was already tried there and reverted -- see that file's own
	 * comment on why it's a worse signal than this one). */
	calldata_set_bool(cd, "hidden", context->source_hidden_in_scene);
	calldata_set_string(cd, "path", context->last_output_path.array ? context->last_output_path.array : "");

	struct dstr hotkey_str;
	dstr_init(&hotkey_str);
	get_source_hotkey_str(obs_filter_get_parent(context->source), "source_record.StartRecording", &hotkey_str);
	calldata_set_string(cd, "hotkey", hotkey_str.array ? hotkey_str.array : "");
	dstr_free(&hotkey_str);
}

static void get_replay_buffer_status_proc(void *data, calldata_t *cd)
{
	struct source_record_filter_context *context = data;
	calldata_set_bool(cd, "enabled", context->replayBuffer);
	calldata_set_bool(cd, "active", context->replayOutput && obs_output_active(context->replayOutput));
	calldata_set_bool(cd, "error", context->replay_error);

	struct dstr hotkey_str;
	dstr_init(&hotkey_str);
	get_output_hotkey_str(context->replayOutput, &hotkey_str);
	calldata_set_string(cd, "hotkey", hotkey_str.array ? hotkey_str.array : "");
	dstr_free(&hotkey_str);
}

static void save_replay_buffer_proc(void *data, calldata_t *cd)
{
	struct source_record_filter_context *context = data;
	bool success = false;
	if (context->replayOutput) {
		proc_handler_t *ph = obs_output_get_proc_handler(context->replayOutput);
		if (ph) {
			calldata_t inner_cd;
			calldata_init(&inner_cd);
			success = proc_handler_call(ph, "save", &inner_cd);
			calldata_free(&inner_cd);
		}
	}
	calldata_set_bool(cd, "success", success);
}

void release_output_stopped(void *data, calldata_t *cd)
{
	UNUSED_PARAMETER(cd);
	struct stop_output *so = data;
	if (!so->context->exiting)
		run_destroy_queued((obs_task_t)obs_output_release, so->output);
	if (so->context->encoder || so->context->audioEncoder[0]) {
		if (so->context->exiting || so->context->closing)
			release_encoders(so->context);
		else
			/* Fix: obs_encoder_release() -> the real hardware/NVENC
			 * session teardown can itself take a real, sometimes
			 * multi-second, amount of time (reported live: OBS's
			 * whole window going white/unresponsive for ~20s, main
			 * stream bitrate dropping to 0 for the duration --
			 * triggered by disabling a Source Record filter via its
			 * Filters-list eye icon, or a scene switch away-and-
			 * quickly-back that hides then re-shows the source).
			 * run_queued() put this on OBS_TASK_GRAPHICS/OBS_TASK_UI
			 * -- the exact same threads the shared video renderer
			 * and every output (including the main stream) depend
			 * on staying responsive -- so a slow encoder teardown
			 * stalled literally everything else sharing that
			 * thread, not just this filter's own recording. Same
			 * root cause, same fix, as the obs_output_release()
			 * line right above (see run_destroy_queued's own
			 * comment for that original ~30s freeze report) --
			 * OBS_TASK_DESTROY is the dedicated background thread
			 * for exactly this kind of expensive teardown work. */
			run_destroy_queued(release_encoders, so->context);
	}
	bfree(data);
}

static void force_stop_output_task(void *data)
{
	struct stop_output *so = data;

	/* If this output was torn down before it ever actually started (still
	 * mid start_replay_task/equivalent, queued via run_queued() but not yet
	 * run -- a duration change or a resize-triggered restart can both race
	 * ahead of it), obs_output_force_stop() below never fires the "stop"
	 * signal: that signal only fires on an active -> inactive transition,
	 * and this output was never active. release_output_stopped (the only
	 * place that actually frees the output/encoder/any buffered frames it
	 * already holds) is connected to exactly that signal, so it would just
	 * never run, leaking all of it for the rest of the process's lifetime.
	 * Skip the signal round-trip entirely and free synchronously right here
	 * instead -- there's nothing running to force-stop in the first place. */
	if (!obs_output_active(so->output)) {
		release_output_stopped(data, NULL);
		return;
	}

	signal_handler_t *sh = obs_output_get_signal_handler(so->output);
	if (sh) {
		signal_handler_connect(sh, "stop", release_output_stopped, data);
	}
	obs_output_force_stop(so->output);
	if (!sh) {
		/* No "stop" signal to wait on, so free right away -- but still
		 * off OBS_TASK_GRAPHICS/OBS_TASK_UI (this function is already
		 * running on one of those two), for the same reason as the
		 * signal-driven path in release_output_stopped() above --
		 * including release_encoders, not just obs_output_release
		 * (see that function's own comment on why: a slow encoder
		 * teardown freezes everything sharing this thread). */
		run_destroy_queued((obs_task_t)obs_output_release, so->output);
		run_destroy_queued(release_encoders, so->context);
		bfree(data);
	}
}

static void start_replay_task(void *data)
{
	struct source_record_filter_context *context = data;
	if (obs_output_start(context->replayOutput)) {
		if (!context->output_active) {
			context->output_active = true;
			obs_source_inc_showing(obs_filter_get_parent(context->source));
		}
	}
	context->starting_replay_output = false;
}

static void ensure_directory(char *path)
{
#ifdef _WIN32
	char *backslash = strrchr(path, '\\');
	if (backslash)
		*backslash = '/';
#endif

	char *slash = strrchr(path, '/');
	if (slash) {
		*slash = 0;
		os_mkdirs(path);
		*slash = '/';
	}

#ifdef _WIN32
	if (backslash)
		*backslash = '\\';
#endif
}

static void remove_filter_task(void *data)
{
	struct source_record_filter_context *filter = data;
	obs_source_t *source = obs_filter_get_parent(filter->source);
	if (!source && filter->view) {
		source = obs_view_get_source(filter->view, SOURCE_CHANNEL);
		obs_source_release(source);
	}
	obs_source_filter_remove(source, filter->source);
}

static void remove_filter(void *data, calldata_t *calldata)
{
	UNUSED_PARAMETER(calldata);
	struct source_record_filter_context *filter = data;
	signal_handler_t *sh = obs_output_get_signal_handler(filter->fileOutput);
	signal_handler_disconnect(sh, "stop", remove_filter, filter);
	/* obs_source_filter_remove() below synchronously destroys the filter
	 * (source_record_filter_destroy frees `filter`) when nothing else holds
	 * an extra ref -- but this callback is connected to the SAME "stop"
	 * signal as release_output_stopped (connected later, at teardown time),
	 * and libobs dispatches signal callbacks in registration order, so
	 * release_output_stopped runs right after this one and would dereference
	 * the `filter` this just freed. Deferring the actual removal to a queued
	 * task lets this "stop" signal's other callbacks finish safely first. */
	run_queued(remove_filter_task, filter);
}

static void stop_output_sync(struct source_record_filter_context *context, obs_output_t *output)
{
	if (!output)
		return;
	signal_handler_t *sh = obs_output_get_signal_handler(output);
	if (sh)
		signal_handler_disconnect(sh, "stop", remove_filter, context);
	if (obs_output_active(output))
		obs_output_force_stop(output);
}

static const char *get_encoder_id(obs_data_t *settings)
{
	const char *enc_id = obs_data_get_string(settings, "encoder");
	if (strlen(enc_id) == 0 || strcmp(enc_id, "x264") == 0 || strcmp(enc_id, "x264_lowcpu") == 0) {
		enc_id = "obs_x264";
	} else if (strcmp(enc_id, "qsv") == 0) {
		enc_id = "obs_qsv11_v2";
	} else if (strcmp(enc_id, "qsv_av1") == 0) {
		enc_id = "obs_qsv11_av1";
	} else if (strcmp(enc_id, "amd") == 0) {
		enc_id = "h264_texture_amf";
	} else if (strcmp(enc_id, "amd_hevc") == 0) {
		enc_id = "h265_texture_amf";
	} else if (strcmp(enc_id, "amd_av1") == 0) {
		enc_id = "av1_texture_amf";
	} else if (strcmp(enc_id, "nvenc") == 0) {
		enc_id = EncoderAvailable("obs_nvenc_h264_tex") ? "obs_nvenc_h264_tex"
								: (EncoderAvailable("jim_nvenc") ? "jim_nvenc" : "ffmpeg_nvenc");
	} else if (strcmp(enc_id, "nvenc_hevc") == 0) {
		enc_id = EncoderAvailable("obs_nvenc_hevc_tex")
				 ? "obs_nvenc_hevc_tex"
				 : (EncoderAvailable("jim_hevc_nvenc") ? "jim_hevc_nvenc" : "ffmpeg_hevc_nvenc");
	} else if (strcmp(enc_id, "nvenc_av1") == 0) {
		enc_id = EncoderAvailable("obs_nvenc_av1_tex") ? "obs_nvenc_av1_tex" : "jim_av1_nvenc";
	} else if (strcmp(enc_id, "apple_h264") == 0) {
		enc_id = "com.apple.videotoolbox.videoencoder.ave.avc";
	} else if (strcmp(enc_id, "apple_hevc") == 0) {
		enc_id = "com.apple.videotoolbox.videoencoder.ave.hevc";
	}
	return enc_id;
}

static void (*obs_encoder_set_gpu_scale_type_func)(obs_encoder_t *encoder, enum obs_scale_type gpu_scale_type) = NULL;

static bool (*obs_encoder_set_frame_rate_divisor_func)(obs_encoder_t *, uint32_t) = NULL;

static void update_video_encoder(struct source_record_filter_context *filter, obs_data_t *settings)
{
	if (obs_encoder_video(filter->encoder) != filter->video_output) {
		if (obs_encoder_active(filter->encoder)) {
			obs_encoder_release(filter->encoder);
			const char *enc_id = get_encoder_id(settings);
			filter->encoder = obs_video_encoder_create(enc_id, obs_source_get_name(filter->source), settings, NULL);
		}
		obs_encoder_set_video(filter->encoder, filter->video_output);
	}
	uint32_t divisor = (uint32_t)obs_data_get_int(settings, "frame_rate_divisor");
	if (divisor > 1 && obs_encoder_set_frame_rate_divisor_func)
		obs_encoder_set_frame_rate_divisor_func(filter->encoder, divisor);
	bool scale = obs_data_get_bool(settings, "scale");
	if (scale) {
		uint32_t width = (uint32_t)obs_data_get_int(settings, "width");
		uint32_t height = (uint32_t)obs_data_get_int(settings, "height");
		if (width > 0 && height > 0) {
			obs_encoder_set_scaled_size(filter->encoder, width, height);
		} else {
			obs_encoder_set_scaled_size(filter->encoder, 0, 0);
		}
		if (obs_encoder_set_gpu_scale_type_func)
			obs_encoder_set_gpu_scale_type_func(filter->encoder,
							    (enum obs_scale_type)obs_data_get_int(settings, "scale_type"));
	} else {
		obs_encoder_set_scaled_size(filter->encoder, 0, 0);
	}
	if (filter->fileOutput && obs_output_get_video_encoder(filter->fileOutput) != filter->encoder)
		obs_output_set_video_encoder(filter->fileOutput, filter->encoder);
	if (filter->streamOutput && obs_output_get_video_encoder(filter->streamOutput) != filter->encoder)
		obs_output_set_video_encoder(filter->streamOutput, filter->encoder);
	if (filter->replayOutput && obs_output_get_video_encoder(filter->replayOutput) != filter->encoder)
		obs_output_set_video_encoder(filter->replayOutput, filter->encoder);
}

static void start_file_output(struct source_record_filter_context *filter, obs_data_t *settings)
{
	obs_data_t *s = obs_data_create();
	char path[512];
	const char *format = obs_data_get_string(settings, "rec_format");
	char *filename =
		os_generate_formatted_filename(GetFormatExt(format), true, obs_data_get_string(settings, "filename_formatting"));
	snprintf(path, 512, "%s/%s", obs_data_get_string(settings, "path"), filename);
	bfree(filename);
	ensure_directory(path);
	obs_data_set_string(s, "path", path);
	dstr_copy(&filter->last_output_path, path);
	obs_data_set_string(s, "directory", obs_data_get_string(settings, "path"));
	obs_data_set_string(s, "format", obs_data_get_string(settings, "filename_formatting"));
	obs_data_set_string(s, "extension", GetFormatExt(format));
	obs_data_set_bool(s, "split_file", obs_data_get_bool(settings, "split_file"));
	obs_data_set_int(s, "max_size_mb", obs_data_get_int(settings, "max_size_mb"));
	obs_data_set_int(s, "max_time_sec", obs_data_get_int(settings, "max_time_sec"));

	const char *output_id = "ffmpeg_muxer";
	if (strcmp(format, "hybrid_mp4") == 0) {
		output_id = "mp4_output";
	} else if (strcmp(format, "hybrid_mov") == 0) {
		output_id = "mov_output";
	}
	if (!filter->fileOutput || strcmp(obs_output_get_id(filter->fileOutput), output_id) != 0) {
		obs_output_release(filter->fileOutput);
		filter->fileOutput = obs_output_create(output_id, obs_source_get_name(filter->source), s, NULL);
		if (filter->fileOutput) {
			signal_handler_t *sh = obs_output_get_signal_handler(filter->fileOutput);
			signal_handler_connect(sh, "stop", source_record_file_stopped, filter);
			if (filter->remove_after_record) {
				signal_handler_connect(sh, "stop", remove_filter, filter);
			}
		}
	} else {
		obs_output_update(filter->fileOutput, s);
	}
	obs_data_release(s);
	if (!filter->fileOutput) {
		/* obs_output_create failed (bad output_id, OOM, disallowed disk,
		 * etc.) -- without this check the filter would silently believe
		 * it's recording forever: starting_file_output would get set below
		 * against a NULL output, and record_error only ever flips on a
		 * "stop" signal that a nonexistent output can never fire. */
		filter->record_error = true;
		return;
	}
	if (filter->encoder) {
		update_video_encoder(filter, settings);
		obs_output_set_video_encoder(filter->fileOutput, filter->encoder);
	}
	for (int i = 0; i < MAX_AUDIO_MIXES; i++) {
		if (!filter->audioEncoder[i])
			continue;

		obs_encoder_set_audio(filter->audioEncoder[i], filter->audio_output);
		obs_output_set_audio_encoder(filter->fileOutput, filter->audioEncoder[i], i);
	}

	filter->starting_file_output = true;
	filter->record_error = false;

	run_queued(start_file_output_task, filter);
}

#define FTL_PROTOCOL "ftl"
#define RTMP_PROTOCOL "rtmp"

static void start_stream_output(struct source_record_filter_context *filter, obs_data_t *settings)
{
	if (!filter->service) {
		const char *server = obs_data_get_string(settings, "server");
		bool whip = strstr(server, "whip") != NULL;
		if (whip)
			obs_data_set_string(settings, "bearer_token", obs_data_get_string(settings, "key"));

		filter->service = obs_service_create(whip ? "whip_custom" : "rtmp_custom", obs_source_get_name(filter->source),
						     settings, NULL);
	} else {
		obs_service_update(filter->service, settings);
	}
	obs_service_apply_encoder_settings(filter->service, settings, NULL);

	const char *type = NULL;
#ifdef _WIN32
	void *handle = os_dlopen("obs");
#else
	void *handle = dlopen(NULL, RTLD_LAZY);
#endif
	if (handle) {
		const char *(*type_func)(obs_service_t *) =
			(const char *(*)(obs_service_t *))os_dlsym(handle, "obs_service_get_output_type");
		if (!type_func)
			type_func = (const char *(*)(obs_service_t *))os_dlsym(handle, "obs_service_get_preferred_output_type");
		if (type_func) {
			type = type_func(filter->service);
		}
		if (!type) {
			const char *url = NULL;
			const char *(*url_func)(obs_service_t *) =
				(const char *(*)(obs_service_t *))os_dlsym(handle, "obs_service_get_url");
			if (url_func) {
				url = url_func(filter->service);
			} else {
				const char *(*info_func)(obs_service_t *, uint32_t) =
					(const char *(*)(obs_service_t *, uint32_t))os_dlsym(handle,
											     "obs_service_get_connect_info");
				if (info_func)
					url = info_func(filter->service, 0); // OBS_SERVICE_CONNECT_INFO_SERVER_URL
			}
			type = "rtmp_output";
			if (url != NULL && strncmp(url, "ftl", 3) == 0) {
				type = "ftl_output";
			} else if (url != NULL && strncmp(url, "rtmp", 4) != 0) {
				type = "ffmpeg_mpegts_muxer";
			}
		}
		os_dlclose(handle);
	} else {
		type = "rtmp_output";
	}

	if (!filter->streamOutput) {
		filter->streamOutput = obs_output_create(type, obs_source_get_name(filter->source), settings, NULL);
		if (!filter->streamOutput) {
			/* Same silent-stuck-state risk as start_file_output's own
			 * creation-failure check -- without this, the filter would
			 * mark itself as streaming with a NULL output and no way to
			 * ever report failure back out. */
			return;
		}
	} else {
		obs_output_update(filter->streamOutput, settings);
	}
	obs_output_set_service(filter->streamOutput, filter->service);

	if (filter->encoder) {
		update_video_encoder(filter, settings);
		obs_output_set_video_encoder(filter->streamOutput, filter->encoder);
	}

	for (int i = 0; i < MAX_AUDIO_MIXES; i++) {
		if (!filter->audioEncoder[i])
			continue;
		obs_encoder_set_audio(filter->audioEncoder[i], filter->audio_output);
		obs_output_set_audio_encoder(filter->streamOutput, filter->audioEncoder[i], i);
	}

	filter->starting_stream_output = true;

	run_queued(start_stream_output_task, filter);
}

static void start_replay_output(struct source_record_filter_context *filter, obs_data_t *settings)
{
	obs_data_t *s = obs_data_create();

	obs_data_set_string(s, "directory", obs_data_get_string(settings, "path"));
	obs_data_set_string(s, "format", obs_data_get_string(settings, "replay_filename_formatting"));
	obs_data_set_string(s, "extension", GetFormatExt(obs_data_get_string(settings, "rec_format")));
	obs_data_set_bool(s, "allow_spaces", true);
	filter->replay_buffer_duration = obs_data_get_int(settings, "replay_duration");
	obs_data_set_int(s, "max_time_sec", filter->replay_buffer_duration);
	obs_data_set_int(s, "max_size_mb", 10000);
	if (!filter->replayOutput) {
		obs_data_t *hotkeys = obs_data_get_obj(settings, "replay_hotkeys");
		struct dstr name;
		obs_source_t *parent = obs_filter_get_parent(filter->source);
		if (parent) {
			dstr_init_copy(&name, obs_source_get_name(parent));
			dstr_cat(&name, " - ");
			dstr_cat(&name, obs_source_get_name(filter->source));
		} else {
			dstr_init_copy(&name, obs_source_get_name(filter->source));
		}

		filter->replayOutput = obs_output_create("replay_buffer", name.array, s, hotkeys);
		filter->replay_error = !filter->replayOutput;
		signal_handler_t *sh = obs_output_get_signal_handler(filter->replayOutput);
		if (sh) {
			signal_handler_connect(sh, "saved", source_record_replay_saved, filter);
			signal_handler_connect(sh, "stop", source_record_replay_stopped, filter);
			if (filter->remove_after_record) {
				signal_handler_connect(sh, "stop", remove_filter, filter);
			}
		}
		dstr_free(&name);
		obs_data_release(hotkeys);
		if (!filter->replayOutput) {
			/* Same silent-stuck-state risk as start_file_output's own
			 * creation-failure check -- bail before starting_replay_output
			 * gets set and the dereferences below run against NULL. */
			obs_data_release(s);
			return;
		}
	} else {
		obs_output_update(filter->replayOutput, s);
	}
	obs_data_release(s);
	if (filter->encoder) {
		update_video_encoder(filter, settings);
	}
	for (int i = 0; i < MAX_AUDIO_MIXES; i++) {
		if (!filter->audioEncoder[i])
			continue;
		obs_encoder_set_audio(filter->audioEncoder[i], filter->audio_output);

		if (obs_output_get_audio_encoder(filter->replayOutput, i) != filter->audioEncoder[i])
			obs_output_set_audio_encoder(filter->replayOutput, filter->audioEncoder[i], i);
	}

	filter->starting_replay_output = true;

	run_queued(start_replay_task, filter);
}

static void copy_defaults(obs_data_t *from, obs_data_t *to)
{
	for (obs_data_item_t *default_item = obs_data_first(from); default_item != NULL; obs_data_item_next(&default_item)) {
		if (!obs_data_item_has_default_value(default_item))
			continue;
		enum obs_data_type item_type = obs_data_item_gettype(default_item);
		const char *name = obs_data_item_get_name(default_item);
		if (item_type == OBS_DATA_STRING) {
			obs_data_set_default_string(to, name, obs_data_item_get_default_string(default_item));
		} else if (item_type == OBS_DATA_NUMBER) {
			enum obs_data_number_type num_type = obs_data_item_numtype(default_item);
			if (num_type == OBS_DATA_NUM_INT) {
				obs_data_set_default_int(to, name, obs_data_item_get_default_int(default_item));
			} else if (num_type == OBS_DATA_NUM_DOUBLE) {
				obs_data_set_default_double(to, name, obs_data_item_get_default_double(default_item));
			}
		} else if (item_type == OBS_DATA_BOOLEAN) {
			obs_data_set_default_bool(to, name, obs_data_item_get_default_bool(default_item));
		}
	}
}

static void set_encoder_defaults(obs_data_t *settings)
{
	obs_data_t *enc_defaults = obs_encoder_defaults(get_encoder_id(settings));
	if (enc_defaults) {
		copy_defaults(enc_defaults, settings);
		obs_data_release(enc_defaults);
	}
	const char *enc_id = obs_data_get_string(settings, "audio_encoder");
	if (!enc_id || !strlen(enc_id))
		enc_id = "ffmpeg_aac";
	enc_defaults = obs_encoder_defaults(enc_id);
	if (enc_defaults) {
		if (obs_data_has_default_value(enc_defaults, "bitrate")) {
			obs_data_set_default_int(settings, "audio_bitrate", obs_data_get_default_int(enc_defaults, "bitrate"));
			obs_data_unset_default_value(enc_defaults, "bitrate");
		}
		copy_defaults(enc_defaults, settings);
		obs_data_release(enc_defaults);
	}
}

static uint32_t get_audio_track_mask(obs_data_t *settings)
{
	uint32_t mask = 0;
	char name[16];
	for (int i = 0; i < MAX_AUDIO_MIXES; i++) {
		snprintf(name, sizeof(name), "audio_track_%d", i + 1);
		if (obs_data_get_bool(settings, name))
			mask |= (1u << i);
	}
	return mask;
}

static void update_encoder(struct source_record_filter_context *filter, obs_data_t *settings)
{
	const char *enc_id = get_encoder_id(settings);
	const bool need_new_encoder = !filter->encoder || strcmp(obs_encoder_get_id(filter->encoder), enc_id) != 0;
	if (need_new_encoder && filter->encoder && obs_encoder_active(filter->encoder)) {
		/* Fix: an active encoder is never released here. Releasing an
		 * in-use encoder caused crashes; it is swapped once idle. */
	} else if (need_new_encoder) {
		obs_encoder_release(filter->encoder);
		filter->encoder = NULL;
		set_encoder_defaults(settings);
		filter->encoder = obs_video_encoder_create(enc_id, obs_source_get_name(filter->source), settings, NULL);
		/* Diagnostic: which real encoder a filter ended up on is otherwise
		 * invisible until something goes wrong (e.g. a filter accidentally
		 * inheriting/landing on a hardware encoder already under load from
		 * the main output or other filters -- see check_encoder_overload).
		 * One line per encoder (re)creation, not per frame. */
		blog(LOG_INFO, "[Source Record] '%s': using encoder '%s'%s", obs_source_get_name(filter->source), enc_id,
		     filter->encoder ? "" : " (FAILED TO CREATE)");

		obs_encoder_set_video(filter->encoder, filter->video_output);
		uint32_t divisor = (uint32_t)obs_data_get_int(settings, "frame_rate_divisor");
		if (divisor > 1 && obs_encoder_set_frame_rate_divisor_func)
			obs_encoder_set_frame_rate_divisor_func(filter->encoder, divisor);
		bool scale = obs_data_get_bool(settings, "scale");
		if (scale) {
			uint32_t width = (uint32_t)obs_data_get_int(settings, "width");
			uint32_t height = (uint32_t)obs_data_get_int(settings, "height");
			if (width > 0 && height > 0) {
				obs_encoder_set_scaled_size(filter->encoder, width, height);
			} else {
				obs_encoder_set_scaled_size(filter->encoder, 0, 0);
			}
			if (obs_encoder_set_gpu_scale_type_func)
				obs_encoder_set_gpu_scale_type_func(filter->encoder,
								    (enum obs_scale_type)obs_data_get_int(settings, "scale_type"));
		} else {
			obs_encoder_set_scaled_size(filter->encoder, 0, 0);
		}
		if (filter->fileOutput && obs_output_get_video_encoder(filter->fileOutput) != filter->encoder)
			obs_output_set_video_encoder(filter->fileOutput, filter->encoder);
		if (filter->streamOutput && obs_output_get_video_encoder(filter->streamOutput) != filter->encoder)
			obs_output_set_video_encoder(filter->streamOutput, filter->encoder);
		if (filter->replayOutput && obs_output_get_video_encoder(filter->replayOutput) != filter->encoder)
			obs_output_set_video_encoder(filter->replayOutput, filter->encoder);
	} else if (!obs_encoder_active(filter->encoder)) {
		/* Fix: re-point the encoder at the current video_output in case
		 * the view was recreated after a parent size change. */
		obs_encoder_set_video(filter->encoder, filter->video_output);
		obs_encoder_update(filter->encoder, settings);
	}
	const int audio_track = obs_data_get_bool(settings, "different_audio") ? (int)obs_data_get_int(settings, "audio_track") : 0;
	const uint32_t audio_track_mask = audio_track == AUDIO_TRACK_CUSTOM ? get_audio_track_mask(settings) : 0;
	const bool uses_master_audio = audio_track > 0 || audio_track == -1 || audio_track == AUDIO_TRACK_CUSTOM;
	const bool used_master_audio =
		filter->audio_track > 0 || filter->audio_track == -1 || filter->audio_track == AUDIO_TRACK_CUSTOM;
	if (filter->closing) {
		if (!used_master_audio && filter->audio_output) {
			audio_output_close(filter->audio_output);
			filter->audio_output = NULL;
		}
	} else if (!filter->audio_output) {
		if (uses_master_audio) {
			filter->audio_output = obs_get_audio();
		} else {
			struct audio_output_info oi = {0};
			oi.name = obs_source_get_name(filter->source);
			oi.speakers = SPEAKERS_STEREO;
			oi.samples_per_sec = audio_output_get_sample_rate(obs_get_audio());
			oi.format = AUDIO_FORMAT_FLOAT_PLANAR;
			oi.input_param = filter;
			oi.input_callback = audio_input_callback;
			audio_output_open(&filter->audio_output, &oi);
		}
	} else if (uses_master_audio && !used_master_audio) {
		audio_output_close(filter->audio_output);
		filter->audio_output = obs_get_audio();
	} else if (!uses_master_audio && used_master_audio) {
		filter->audio_output = NULL;
		struct audio_output_info oi = {0};
		oi.name = obs_source_get_name(filter->source);
		oi.speakers = SPEAKERS_STEREO;
		oi.samples_per_sec = audio_output_get_sample_rate(obs_get_audio());
		oi.format = AUDIO_FORMAT_FLOAT_PLANAR;
		oi.input_param = filter;
		oi.input_callback = audio_input_callback;
		audio_output_open(&filter->audio_output, &oi);
	}

	if (!filter->audioEncoder[0] || filter->audio_track != audio_track ||
	    (audio_track == AUDIO_TRACK_CUSTOM && filter->audio_track_mask != audio_track_mask)) {
		for (int i = 0; i < MAX_AUDIO_MIXES; i++) {
			if (!filter->audioEncoder[i])
				continue;
			obs_encoder_release(filter->audioEncoder[i]);
			filter->audioEncoder[i] = NULL;
		}
		enc_id = obs_data_get_string(settings, "audio_encoder");
		if (!enc_id || !strlen(enc_id))
			enc_id = "ffmpeg_aac";

		obs_data_t *audio_settings = obs_data_create();
		if (obs_data_has_user_value(settings, "audio_bitrate") || obs_data_has_default_value(settings, "audio_bitrate")) {
			obs_data_set_int(audio_settings, "bitrate", obs_data_get_int(settings, "audio_bitrate"));
		}
		if (audio_track > 0) {
			filter->audioEncoder[0] = obs_audio_encoder_create(enc_id, obs_source_get_name(filter->source),
									   audio_settings, audio_track - 1, NULL);
		} else if (audio_track == -1) {
			struct dstr name;
			dstr_init(&name);
			for (int i = 0; i < MAX_AUDIO_MIXES; i++) {
				dstr_printf(&name, "%s %d", obs_module_text("Track"), i + 1);
				filter->audioEncoder[i] = obs_audio_encoder_create(enc_id, name.array, audio_settings, i, NULL);
			}
			dstr_free(&name);
		} else if (audio_track == AUDIO_TRACK_CUSTOM) {
			struct dstr name;
			dstr_init(&name);
			int out_idx = 0;
			for (int i = 0; i < MAX_AUDIO_MIXES; i++) {
				if ((audio_track_mask & (1u << i)) == 0)
					continue;
				dstr_printf(&name, "%s %d", obs_module_text("Track"), i + 1);
				filter->audioEncoder[out_idx] = obs_audio_encoder_create(enc_id, name.array, audio_settings, i, NULL);
				out_idx++;
			}
			dstr_free(&name);
		} else {
			filter->audioEncoder[0] =
				obs_audio_encoder_create(enc_id, obs_source_get_name(filter->source), audio_settings, 0, NULL);
		}
		obs_data_release(audio_settings);
		for (int i = 0; i < MAX_AUDIO_MIXES; i++) {
			if (!filter->audioEncoder[i])
				continue;
			if (filter->audio_output)
				obs_encoder_set_audio(filter->audioEncoder[i], filter->audio_output);

			if (filter->fileOutput)
				obs_output_set_audio_encoder(filter->fileOutput, filter->audioEncoder[i], i);
			if (filter->replayOutput)
				obs_output_set_audio_encoder(filter->replayOutput, filter->audioEncoder[i], i);
		}
	}
	filter->audio_track = audio_track;
	filter->audio_track_mask = audio_track_mask;
}

/* Rebuilds the private overlay scene from the reference scene's items, copying
 * each item's real position/scale/bounds/crop so it lines up the same way it
 * does in the reference scene, rescaled from the OBS canvas to this filter's
 * own (usually differently sized) output canvas. */
static void update_overlay_sources(struct source_record_filter_context *filter, obs_data_t *settings)
{
	const char *scene_name = obs_data_get_string(settings, "overlay_scene");
	obs_source_t *ref_source = (scene_name && strlen(scene_name)) ? obs_get_source_by_name(scene_name) : NULL;
	obs_scene_t *ref_scene = ref_source ? obs_scene_from_source(ref_source) : NULL;

	obs_scene_t *new_scene = obs_scene_create_private("Source Record Overlay");

	if (ref_scene) {
		struct obs_video_info ovi = {0};
		obs_get_video_info(&ovi);
		const float sx = (ovi.base_width && filter->width) ? (float)filter->width / (float)ovi.base_width : 1.0f;
		const float sy = (ovi.base_height && filter->height) ? (float)filter->height / (float)ovi.base_height : 1.0f;

		char prop_name[24];
		for (int i = 1; i <= MAX_OVERLAY_SOURCES; i++) {
			snprintf(prop_name, sizeof(prop_name), "overlay_source_%d", i);
			const char *item_name = obs_data_get_string(settings, prop_name);
			if (!item_name || !strlen(item_name))
				continue;
			obs_sceneitem_t *ref_item = obs_scene_find_source(ref_scene, item_name);
			if (!ref_item)
				continue;
			obs_source_t *item_source = obs_sceneitem_get_source(ref_item);
			if (!item_source)
				continue;

			obs_sceneitem_t *new_item = obs_scene_add(new_scene, item_source);
			if (!new_item)
				continue;

			struct vec2 pos, scale, bounds;
			obs_sceneitem_get_pos(ref_item, &pos);
			obs_sceneitem_get_scale(ref_item, &scale);
			obs_sceneitem_get_bounds(ref_item, &bounds);
			pos.x *= sx;
			pos.y *= sy;
			scale.x *= sx;
			scale.y *= sy;
			bounds.x *= sx;
			bounds.y *= sy;

			obs_sceneitem_set_rot(new_item, obs_sceneitem_get_rot(ref_item));
			obs_sceneitem_set_alignment(new_item, obs_sceneitem_get_alignment(ref_item));
			obs_sceneitem_set_bounds_type(new_item, obs_sceneitem_get_bounds_type(ref_item));
			obs_sceneitem_set_bounds_alignment(new_item, obs_sceneitem_get_bounds_alignment(ref_item));
			obs_sceneitem_set_bounds(new_item, &bounds);
			obs_sceneitem_set_scale(new_item, &scale);
			obs_sceneitem_set_pos(new_item, &pos);

			struct obs_sceneitem_crop crop;
			obs_sceneitem_get_crop(ref_item, &crop);
			obs_sceneitem_set_crop(new_item, &crop);
		}
	}

	if (ref_source)
		obs_source_release(ref_source);

	obs_view_set_source(filter->view, OVERLAY_CHANNEL, obs_scene_get_source(new_scene));

	if (filter->overlay_scene)
		obs_scene_release(filter->overlay_scene);
	filter->overlay_scene = new_scene;
}

static void source_record_filter_update(void *data, obs_data_t *settings)
{
	struct source_record_filter_context *filter = data;
	obs_source_t *parent = obs_filter_get_parent(filter->source);
	if (obs_obj_is_private(parent)) {
		filter->closing = true;
		return;
	}
	if (obs_data_get_bool(settings, "scale")) {
		const char *res = obs_data_get_string(settings, "resolution");
		uint32_t width, height;
		if (sscanf(res, "%dx%d", &width, &height) == 2 && width > 0 && height > 0) {
			obs_data_set_int(settings, "width", width);
			obs_data_set_int(settings, "height", height);
		} else {
			struct dstr str;
			dstr_init(&str);
			dstr_printf(&str, "%dx%d", (int)obs_data_get_int(settings, "width"),
				    (int)obs_data_get_int(settings, "height"));
			obs_data_set_string(settings, "resolution", str.array);
			dstr_free(&str);
		}
	}
	filter->remove_after_record = obs_data_get_bool(settings, "remove_after_record");
	filter->record_max_seconds = obs_data_get_int(settings, "record_max_seconds");
	const long long record_mode = obs_data_get_int(settings, "record_mode");
	const long long stream_mode = obs_data_get_int(settings, "stream_mode");
	const bool replay_buffer = obs_data_get_bool(settings, "replay_buffer") && !filter->closing;
	if (!filter->closing && (record_mode != OUTPUT_MODE_NONE || stream_mode != OUTPUT_MODE_NONE || replay_buffer)) {
		update_encoder(filter, settings);
	}
	bool record = false;
	if (filter->closing) {
	} else if (record_mode == OUTPUT_MODE_ALWAYS) {
		record = true;
	} else if (record_mode == OUTPUT_MODE_RECORDING) {
		record = obs_frontend_recording_active() && filter->last_frontend_event != OBS_FRONTEND_EVENT_RECORDING_STOPPING &&
			 filter->last_frontend_event != OBS_FRONTEND_EVENT_RECORDING_STOPPED;
	} else if (record_mode == OUTPUT_MODE_STREAMING) {
		record = obs_frontend_streaming_active() && filter->last_frontend_event != OBS_FRONTEND_EVENT_STREAMING_STOPPING &&
			 filter->last_frontend_event != OBS_FRONTEND_EVENT_STREAMING_STOPPED;
	} else if (record_mode == OUTPUT_MODE_STREAMING_OR_RECORDING) {
		record = (obs_frontend_streaming_active() && filter->last_frontend_event != OBS_FRONTEND_EVENT_STREAMING_STOPPING &&
			  filter->last_frontend_event != OBS_FRONTEND_EVENT_STREAMING_STOPPED) ||
			 (obs_frontend_recording_active() && filter->last_frontend_event != OBS_FRONTEND_EVENT_RECORDING_STOPPING &&
			  filter->last_frontend_event != OBS_FRONTEND_EVENT_RECORDING_STOPPED);
	} else if (record_mode == OUTPUT_MODE_VIRTUAL_CAMERA) {
		record = obs_frontend_virtualcam_active() && filter->last_frontend_event != OBS_FRONTEND_EVENT_VIRTUALCAM_STOPPED;
	}

	if (parent && filter->view && (record || replay_buffer)) {
		obs_source_t *view_source = obs_view_get_source(filter->view, SOURCE_CHANNEL);
		if (view_source != parent)
			obs_view_set_source(filter->view, SOURCE_CHANNEL, parent);
		obs_source_release(view_source);
	}

	if (parent && filter->view && !filter->closing) {
		obs_source_t *background_source = obs_view_get_source(filter->view, BACKGROUND_CHANNEL);
		if (!background_source) {
			background_source = obs_source_create_private("color_source", "Source Record Background", NULL);
			obs_view_set_source(filter->view, BACKGROUND_CHANNEL, background_source);
		}
		obs_data_t *css = obs_source_get_settings(background_source);
		if (obs_data_get_int(css, "color") != obs_data_get_int(settings, "backgroundColor") ||
		    obs_data_get_int(css, "width") != obs_source_get_width(parent) ||
		    obs_data_get_int(css, "height") != obs_source_get_height(parent)) {
			obs_data_set_int(css, "color", obs_data_get_int(settings, "backgroundColor"));
			obs_data_set_int(css, "width", obs_source_get_width(parent));
			obs_data_set_int(css, "height", obs_source_get_height(parent));
			obs_source_update(background_source, css);
		}
		obs_data_release(css);
		obs_source_release(background_source);

		update_overlay_sources(filter, settings);
	}

	if (record != filter->record) {
		if (record) {
			if (obs_source_enabled(filter->source) && filter->video_output)
				start_file_output(filter, settings);
		} else if (filter->fileOutput) {
			if (filter->closing) {
				stop_output_sync(filter, filter->fileOutput);
			} else {
				struct stop_output *so = bmalloc(sizeof(struct stop_output));
				so->output = filter->fileOutput;
				so->context = filter;
				run_queued(force_stop_output_task, so);
				filter->fileOutput = NULL;
			}
		}
		filter->record = record;
	}

	if (record && filter->fileOutput && filter->last_frontend_event == OBS_FRONTEND_EVENT_RECORDING_PAUSED &&
	    !obs_output_paused(filter->fileOutput)) {
		obs_output_pause(filter->fileOutput, true);
		filter->last_frontend_event = -1;
	} else if (record && filter->fileOutput && filter->last_frontend_event == OBS_FRONTEND_EVENT_RECORDING_UNPAUSED &&
		   obs_output_paused(filter->fileOutput)) {
		obs_output_pause(filter->fileOutput, false);
		filter->last_frontend_event = -1;
	}

	if (replay_buffer != filter->replayBuffer) {
		if (replay_buffer) {
			if (obs_source_enabled(filter->source) && filter->video_output)
				start_replay_output(filter, settings);
		} else if (filter->replayOutput) {
			obs_data_t *hotkeys = obs_hotkeys_save_output(filter->replayOutput);
			obs_data_set_obj(settings, "replay_hotkeys", hotkeys);
			obs_data_release(hotkeys);
			if (filter->closing) {
				stop_output_sync(filter, filter->replayOutput);
			} else {
				struct stop_output *so = bmalloc(sizeof(struct stop_output));
				so->output = filter->replayOutput;
				so->context = filter;
				run_queued(force_stop_output_task, so);
				filter->replayOutput = NULL;
			}
		}

		filter->replayBuffer = replay_buffer;
	} else if (replay_buffer && filter->replayOutput && obs_source_enabled(filter->source)) {
		if (filter->replay_buffer_duration != obs_data_get_int(settings, "replay_duration")) {
			obs_data_t *hotkeys = obs_hotkeys_save_output(filter->replayOutput);
			obs_data_set_obj(settings, "replay_hotkeys", hotkeys);
			obs_data_release(hotkeys);
			struct stop_output *so = bmalloc(sizeof(struct stop_output));
			so->output = filter->replayOutput;
			so->context = filter;
			run_queued(force_stop_output_task, so);
			filter->replayOutput = NULL;
			start_replay_output(filter, settings);
		}
		obs_data_t *replay_settings = obs_output_get_settings(filter->replayOutput);
		obs_data_set_string(replay_settings, "format", obs_data_get_string(settings, "replay_filename_formatting"));
		obs_data_release(replay_settings);
	}

	bool stream = false;
	if (filter->closing) {
	} else if (stream_mode == OUTPUT_MODE_ALWAYS) {
		stream = true;
	} else if (stream_mode == OUTPUT_MODE_RECORDING) {
		stream = obs_frontend_recording_active() && filter->last_frontend_event != OBS_FRONTEND_EVENT_RECORDING_STOPPING &&
			 filter->last_frontend_event != OBS_FRONTEND_EVENT_RECORDING_STOPPED;
	} else if (stream_mode == OUTPUT_MODE_STREAMING) {
		stream = obs_frontend_streaming_active() && filter->last_frontend_event != OBS_FRONTEND_EVENT_STREAMING_STOPPING &&
			 filter->last_frontend_event != OBS_FRONTEND_EVENT_STREAMING_STOPPED;
	} else if (stream_mode == OUTPUT_MODE_STREAMING_OR_RECORDING) {
		stream = (obs_frontend_streaming_active() && filter->last_frontend_event != OBS_FRONTEND_EVENT_STREAMING_STOPPING &&
			  filter->last_frontend_event != OBS_FRONTEND_EVENT_STREAMING_STOPPED) ||
			 (obs_frontend_recording_active() && filter->last_frontend_event != OBS_FRONTEND_EVENT_RECORDING_STOPPING &&
			  filter->last_frontend_event != OBS_FRONTEND_EVENT_RECORDING_STOPPED);
	} else if (stream_mode == OUTPUT_MODE_VIRTUAL_CAMERA) {
		stream = obs_frontend_virtualcam_active() && filter->last_frontend_event != OBS_FRONTEND_EVENT_VIRTUALCAM_STOPPED;
	}

	if (parent && filter->view && stream) {
		obs_source_t *view_source = obs_view_get_source(filter->view, SOURCE_CHANNEL);
		if (view_source != parent)
			obs_view_set_source(filter->view, SOURCE_CHANNEL, parent);
		obs_source_release(view_source);
	}

	if (stream != filter->stream) {
		if (stream) {
			if (obs_source_enabled(filter->source) && filter->video_output)
				start_stream_output(filter, settings);
		} else if (filter->streamOutput) {
			if (filter->closing) {
				stop_output_sync(filter, filter->streamOutput);
			} else {
				struct stop_output *so = bmalloc(sizeof(struct stop_output));
				so->output = filter->streamOutput;
				so->context = filter;
				run_queued(force_stop_output_task, so);
				filter->streamOutput = NULL;
			}
		}
		filter->stream = stream;
	}

	if (!replay_buffer && !record && !stream) {
		if (filter->encoder && !obs_encoder_active(filter->encoder)) {
			obs_encoder_release(filter->encoder);
			filter->encoder = NULL;
		}
		for (int i = 0; i < MAX_AUDIO_MIXES; i++) {
			if (!filter->audioEncoder[i] || obs_encoder_active(filter->audioEncoder[i]))
				continue;
			obs_encoder_release(filter->audioEncoder[i]);
			filter->audioEncoder[i] = NULL;
		}
	}

	vec4_from_rgba(&filter->backgroundColor, (uint32_t)obs_data_get_int(settings, "backgroundColor"));

	/* Locked for the same reason as audio_input_callback's read -- this can
	 * run on the UI thread or, via the websocket vendor API, an arbitrary
	 * caller thread, concurrently with the audio mixer thread resolving
	 * filter->audio_source. See the field's own comment. */
	pthread_mutex_lock(&filter->audio_source_mutex);
	if (obs_data_get_bool(settings, "different_audio")) {
		const char *source_name = obs_data_get_string(settings, "audio_source");
		if (!strlen(source_name)) {
			if (filter->audio_source) {
				obs_weak_source_release(filter->audio_source);
				filter->audio_source = NULL;
			}
		} else {
			obs_source_t *source = obs_weak_source_get_source(filter->audio_source);
			/* Read the name (and only then release) -- releasing first and
			 * reading obs_source_get_name(source) after was a use-after-free
			 * whenever this ref happened to be the last one (i.e. exactly
			 * the orphaned-weak-ref case being detected here). */
			bool name_changed = !source || strcmp(source_name, obs_source_get_name(source)) != 0;
			if (source)
				obs_source_release(source);
			if (name_changed) {
				if (filter->audio_source) {
					obs_weak_source_release(filter->audio_source);
					filter->audio_source = NULL;
				}
				source = obs_get_source_by_name(source_name);
				if (source) {
					filter->audio_source = obs_source_get_weak_source(source);
					obs_source_release(source);
				}
			}
		}

	} else if (filter->audio_source) {
		obs_weak_source_release(filter->audio_source);
		filter->audio_source = NULL;
	}
	pthread_mutex_unlock(&filter->audio_source_mutex);
}

static void source_record_filter_save(void *data, obs_data_t *settings)
{
	struct source_record_filter_context *filter = data;
	if (filter->replayOutput) {
		obs_data_t *hotkeys = obs_hotkeys_save_output(filter->replayOutput);
		obs_data_set_obj(settings, "replay_hotkeys", hotkeys);
		obs_data_release(hotkeys);
	}
}

static void source_record_filter_defaults(obs_data_t *settings)
{
	config_t *config = obs_frontend_get_profile_config();

	const char *mode = config_get_string(config, "Output", "Mode");
	const char *type = config_get_string(config, "AdvOut", "RecType");
	const char *adv_path = strcmp(type, "Standard") != 0 || strcmp(type, "standard") != 0
				       ? config_get_string(config, "AdvOut", "FFFilePath")
				       : config_get_string(config, "AdvOut", "RecFilePath");
	bool adv_out = strcmp(mode, "Advanced") == 0 || strcmp(mode, "advanced") == 0;
	const char *rec_path = adv_out ? adv_path : config_get_string(config, "SimpleOutput", "FilePath");

	obs_data_set_default_string(settings, "path", rec_path);
	const char *format = config_get_string(config, "Output", "FilenameFormatting");
	obs_data_set_default_string(settings, "filename_formatting", format);
	obs_data_set_default_string(settings, "replay_filename_formatting", format);
	obs_data_set_default_string(settings, "rec_format",
				    config_get_string(config, adv_out ? "AdvOut" : "SimpleOutput", "RecFormat2"));

	obs_data_set_default_int(settings, "backgroundColor", 0);

	// Reverted back to matching the main recording/stream's own encoder
	// choice (briefly changed to always default new filters to Software/x264
	// instead, to avoid piling concurrent NVENC sessions onto a GPU's real
	// per-chip throughput ceiling -- see git history). That tradeoff went
	// the other way in practice: reported live as a friend's CPU pegged at
	// 80% and lagging everything, including their stream, once a filter's
	// own x264 encode was competing with the main output for CPU time
	// instead. Matching the main output's own choice is the safer default
	// again -- if that's already NVENC, at least it's hardware encoding
	// either way, and users who want a specific encoder for a filter can
	// still pick it explicitly. check_encoder_overload (below) stays either
	// way -- detecting/reporting real contention is still useful regardless
	// of which default caused it.
	const char *enc_id;
	if (adv_out) {
		enc_id = config_get_string(config, "AdvOut", "RecEncoder");
		if (strcmp(enc_id, "none") == 0 || strcmp(enc_id, "None") == 0)
			enc_id = config_get_string(config, "AdvOut", "Encoder");
		else if (strcmp(enc_id, "jim_nvenc") == 0 || strcmp(enc_id, "obs_nvenc_h264_tex") == 0)
			enc_id = "nvenc";

	} else {
		const char *quality = config_get_string(config, "SimpleOutput", "RecQuality");
		if (strcmp(quality, "Stream") == 0 || strcmp(quality, "stream") == 0) {
			enc_id = config_get_string(config, "SimpleOutput", "StreamEncoder");
		} else if (strcmp(quality, "Lossless") == 0 || strcmp(quality, "lossless") == 0) {
			enc_id = "ffmpeg_output";
		} else {
			enc_id = config_get_string(config, "SimpleOutput", "RecEncoder");
		}
	}
	obs_data_set_default_string(settings, "encoder", enc_id);

	obs_data_set_default_string(settings, "audio_encoder", "ffmpeg_aac");

	set_encoder_defaults(settings);
	obs_data_set_default_int(settings, "replay_duration", 5);
	obs_data_set_default_int(settings, "max_size_mb", 2048);
	obs_data_set_default_int(settings, "max_time_sec", 15 * 60);
}

static void source_record_filter_filter_remove(void *data, obs_source_t *parent);

static void update_task(void *param)
{
	struct source_record_filter_context *context = param;
	obs_source_update(context->source, NULL);
}

static void frontend_event(enum obs_frontend_event event, void *data)
{
	struct source_record_filter_context *context = data;
	if (event == OBS_FRONTEND_EVENT_STREAMING_STARTING || event == OBS_FRONTEND_EVENT_STREAMING_STARTED ||
	    event == OBS_FRONTEND_EVENT_STREAMING_STOPPING || event == OBS_FRONTEND_EVENT_STREAMING_STOPPED ||
	    event == OBS_FRONTEND_EVENT_RECORDING_STARTING || event == OBS_FRONTEND_EVENT_RECORDING_STARTED ||
	    event == OBS_FRONTEND_EVENT_RECORDING_STOPPING || event == OBS_FRONTEND_EVENT_RECORDING_STOPPED ||
	    event == OBS_FRONTEND_EVENT_VIRTUALCAM_STARTED || event == OBS_FRONTEND_EVENT_VIRTUALCAM_STOPPED ||
	    event == OBS_FRONTEND_EVENT_RECORDING_PAUSED || event == OBS_FRONTEND_EVENT_RECORDING_UNPAUSED) {
		context->last_frontend_event = (int)event;

		obs_queue_task(OBS_TASK_GRAPHICS, update_task, data, false);
	} else if (event == OBS_FRONTEND_EVENT_EXIT || event == OBS_FRONTEND_EVENT_SCRIPTING_SHUTDOWN) {
		context->closing = true;
		context->exiting = true;
		obs_source_update(context->source, NULL);
	} else if (event == OBS_FRONTEND_EVENT_SCENE_COLLECTION_CLEANUP) {
		context->closing = true;
		obs_source_update(context->source, NULL);
	}
}

static void *source_record_filter_create(obs_data_t *settings, obs_source_t *source)
{
	struct source_record_filter_context *context = bzalloc(sizeof(struct source_record_filter_context));
	context->source = source;
	pthread_mutex_init(&context->audio_source_mutex, NULL);

	da_push_back(source_record_filters, &source);
	context->last_frontend_event = -1;
	context->enableHotkey = OBS_INVALID_HOTKEY_PAIR_ID;
	context->pauseHotkeys = OBS_INVALID_HOTKEY_PAIR_ID;
	context->recordHotkeys = OBS_INVALID_HOTKEY_PAIR_ID;
	context->splitHotkey = OBS_INVALID_HOTKEY_ID;
	context->chapterHotkey = OBS_INVALID_HOTKEY_ID;

	proc_handler_t *ph = obs_source_get_proc_handler(source);
	if (ph) {
		proc_handler_add(ph, "void get_replay_buffer_status(out bool enabled, out bool active, out bool error, out string hotkey)",
				 get_replay_buffer_status_proc, context);
		proc_handler_add(ph, "void save_replay_buffer(out bool success)", save_replay_buffer_proc, context);
		proc_handler_add(
			ph,
			"void get_record_status(out bool enabled, out bool active, out bool error, out bool hidden, out string path, out string hotkey)",
			get_record_status_proc, context);
	}

	signal_handler_t *filter_sh = obs_source_get_signal_handler(source);
	if (filter_sh)
		signal_handler_add(filter_sh, "void replay_saved(string path)");

	source_record_filter_update(context, settings);
	obs_frontend_add_event_callback(frontend_event, context);
	return context;
}

static void source_record_filter_destroy(void *data)
{
	struct source_record_filter_context *context = data;
	da_erase_item(source_record_filters, &context->source);
	context->closing = true;
	if (context->output_active) {
		obs_source_t *parent = obs_filter_get_parent(context->source);
		if (parent)
			obs_source_dec_showing(parent);
		context->output_active = false;
	}
	obs_frontend_remove_event_callback(frontend_event, context);

	stop_output_sync(context, context->fileOutput);
	stop_output_sync(context, context->streamOutput);
	stop_output_sync(context, context->replayOutput);

	if (context->enableHotkey != OBS_INVALID_HOTKEY_PAIR_ID)
		obs_hotkey_pair_unregister(context->enableHotkey);

	if (context->pauseHotkeys != OBS_INVALID_HOTKEY_PAIR_ID)
		obs_hotkey_pair_unregister(context->pauseHotkeys);

	if (context->recordHotkeys != OBS_INVALID_HOTKEY_PAIR_ID)
		obs_hotkey_pair_unregister(context->recordHotkeys);

	if (context->splitHotkey != OBS_INVALID_HOTKEY_ID)
		obs_hotkey_unregister(context->splitHotkey);

	if (context->chapterHotkey != OBS_INVALID_HOTKEY_ID)
		obs_hotkey_unregister(context->chapterHotkey);

	/* Same reasoning as release_output_stopped's obs_output_release call
	 * (see run_destroy_queued's comment near the top of this file) -- this
	 * function runs synchronously on whatever thread destroys the filter
	 * (removing it from the Filters panel, or OBS closing with the source
	 * still present, both typically the UI thread), and a replayOutput can
	 * hold up to ~10GB of buffered packets. Releasing it inline here
	 * reintroduced the exact multi-second freeze the OBS_TASK_DESTROY fix
	 * elsewhere in this file was meant to eliminate -- this call path was
	 * simply missed the first time. These only need the raw obs_output_t
	 * handles, not `context` (which gets freed below), so deferring them is
	 * safe even though `context` won't outlive this function. */
	run_destroy_queued((obs_task_t)obs_output_release, context->fileOutput);
	run_destroy_queued((obs_task_t)obs_output_release, context->streamOutput);
	run_destroy_queued((obs_task_t)obs_output_release, context->replayOutput);
	context->fileOutput = NULL;
	context->streamOutput = NULL;
	context->replayOutput = NULL;

	/* Same bug, same fix, as the three obs_output_release calls right above
	 * (whose own comment explains the general reasoning) -- this one was
	 * ALSO missed the first time: obs_encoder_release can itself be slow
	 * (a real hardware/NVENC session teardown, not just a free -- see
	 * release_output_stopped's own comment on this exact cost), and this
	 * whole function still runs synchronously on the thread destroying the
	 * filter (removing it from the Filters panel, or OBS closing with it
	 * still present) -- freezing that thread for however long it takes.
	 * Only the raw obs_encoder_t handles are needed, not `context` (freed
	 * right after this function returns), so deferring is safe. */
	for (int i = 0; i < MAX_AUDIO_MIXES; i++) {
		run_destroy_queued((obs_task_t)obs_encoder_release, context->audioEncoder[i]);
		context->audioEncoder[i] = NULL;
	}
	run_destroy_queued((obs_task_t)obs_encoder_release, context->encoder);
	context->encoder = NULL;

	pthread_mutex_lock(&context->audio_source_mutex);
	obs_weak_source_release(context->audio_source);
	context->audio_source = NULL;
	pthread_mutex_unlock(&context->audio_source_mutex);
	pthread_mutex_destroy(&context->audio_source_mutex);

	dstr_free(&context->last_output_path);

	if (context->audio_track == 0 && context->audio_output)
		audio_output_close(context->audio_output);
	context->audio_output = NULL;

	obs_service_release(context->service);
	context->service = NULL;

	if (context->view) {
		obs_view_set_source(context->view, BACKGROUND_CHANNEL, NULL);
		obs_view_set_source(context->view, SOURCE_CHANNEL, NULL);
		obs_view_set_source(context->view, OVERLAY_CHANNEL, NULL);
		if (context->video_output) {
			obs_view_remove(context->view);
			context->video_output = NULL;
		}
		obs_view_destroy(context->view);
		context->view = NULL;
	}

	if (context->overlay_scene) {
		obs_scene_release(context->overlay_scene);
		context->overlay_scene = NULL;
	}

	context->source = NULL;

	/* Must run after every other release above (the output releases at
	 * the top of this function are async too, on the same queue) and
	 * right before the actual free -- see wait_for_destroy_queue_drain's
	 * own comment for why this is the fix for a real cross-thread UAF,
	 * not just belt-and-suspenders. */
	wait_for_destroy_queue_drain();
	bfree(context);
}

static bool source_record_enable_hotkey(void *data, obs_hotkey_pair_id id, obs_hotkey_t *hotkey, bool pressed)
{
	UNUSED_PARAMETER(id);
	UNUSED_PARAMETER(hotkey);
	struct source_record_filter_context *context = data;
	if (!pressed)
		return false;

	if (obs_source_enabled(context->source))
		return false;

	obs_source_set_enabled(context->source, true);
	return true;
}

static bool source_record_disable_hotkey(void *data, obs_hotkey_pair_id id, obs_hotkey_t *hotkey, bool pressed)
{
	UNUSED_PARAMETER(id);
	UNUSED_PARAMETER(hotkey);
	struct source_record_filter_context *context = data;
	if (!pressed)
		return false;
	if (!obs_source_enabled(context->source))
		return false;
	obs_source_set_enabled(context->source, false);
	return true;
}

/* Same "start_mode = ALWAYS" / "record_mode = NONE" settings update that
 * websocket_start_record/websocket_stop_record (and the REST-ish vendor
 * requests they back) apply -- these just apply it directly to this
 * filter's own context->source rather than resolving a filter by name off a
 * caller-supplied source, since the hotkey callback already has the exact
 * filter instance it fired on. */
static bool source_record_start_record_hotkey(void *data, obs_hotkey_pair_id id, obs_hotkey_t *hotkey, bool pressed)
{
	UNUSED_PARAMETER(id);
	UNUSED_PARAMETER(hotkey);
	struct source_record_filter_context *context = data;
	if (!pressed)
		return false;

	if (context->record)
		return false;

	obs_data_t *settings = obs_data_create();
	obs_data_set_int(settings, "record_mode", OUTPUT_MODE_ALWAYS);
	obs_source_update(context->source, settings);
	obs_data_release(settings);
	return true;
}

static bool source_record_stop_record_hotkey(void *data, obs_hotkey_pair_id id, obs_hotkey_t *hotkey, bool pressed)
{
	UNUSED_PARAMETER(id);
	UNUSED_PARAMETER(hotkey);
	struct source_record_filter_context *context = data;
	if (!pressed)
		return false;

	if (!context->record)
		return false;

	obs_data_t *settings = obs_data_create();
	obs_data_set_int(settings, "record_mode", OUTPUT_MODE_NONE);
	obs_source_update(context->source, settings);
	obs_data_release(settings);
	return true;
}

static bool source_record_pause_hotkey(void *data, obs_hotkey_pair_id id, obs_hotkey_t *hotkey, bool pressed)
{
	UNUSED_PARAMETER(id);
	UNUSED_PARAMETER(hotkey);
	struct source_record_filter_context *context = data;
	if (!pressed)
		return false;

	if (obs_output_paused(context->fileOutput))
		return false;

	obs_output_pause(context->fileOutput, true);
	return true;
}

static bool source_record_unpause_hotkey(void *data, obs_hotkey_pair_id id, obs_hotkey_t *hotkey, bool pressed)
{
	UNUSED_PARAMETER(id);
	UNUSED_PARAMETER(hotkey);
	struct source_record_filter_context *context = data;
	if (!pressed)
		return false;
	if (!obs_output_paused(context->fileOutput))
		return false;

	obs_output_pause(context->fileOutput, false);
	return true;
}

static void source_record_split_hotkey(void *data, obs_hotkey_id id, obs_hotkey_t *hotkey, bool pressed)
{
	UNUSED_PARAMETER(id);
	UNUSED_PARAMETER(hotkey);
	if (!pressed)
		return;
	struct source_record_filter_context *context = data;
	if (!context->fileOutput)
		return;
	proc_handler_t *ph = obs_output_get_proc_handler(context->fileOutput);
	struct calldata cd;
	calldata_init(&cd);
	proc_handler_call(ph, "split_file", &cd);
	calldata_free(&cd);
}

static void source_record_chapter_hotkey(void *data, obs_hotkey_id id, obs_hotkey_t *hotkey, bool pressed)
{
	UNUSED_PARAMETER(id);
	UNUSED_PARAMETER(hotkey);
	if (!pressed)
		return;
	struct source_record_filter_context *context = data;
	if (!context->fileOutput)
		return;
	proc_handler_t *ph = obs_output_get_proc_handler(context->fileOutput);
	struct calldata cd;
	calldata_init(&cd);
	proc_handler_call(ph, "add_chapter", &cd);
	calldata_free(&cd);
}

// True if `parent` isn't visible (or isn't present at all) in the currently
// active/program scene. Not being in that scene at all counts as hidden too.
static bool source_hidden_in_current_scene(obs_source_t *parent)
{
	obs_source_t *current_scene_source = obs_frontend_get_current_scene();
	if (!current_scene_source)
		return false; // no info available; don't force a change either way

	bool hidden = true;
	obs_scene_t *scene = obs_scene_from_source(current_scene_source);
	if (scene) {
		const char *name = obs_source_get_name(parent);
		obs_sceneitem_t *item = name ? obs_scene_find_source_recursive(scene, name) : NULL;
		if (item)
			hidden = !obs_sceneitem_visible(item);
	}
	obs_source_release(current_scene_source);
	return hidden;
}

// Forces record_mode to None (and disables the filter itself, for the eye
// icon -- see below) while the source is hidden in the current scene, and
// restores both once it's shown again (record_mode staying None if that's
// what it already was).
//
// Enforces the invariant every 0.5s, not just once on the hidden/shown
// TRANSITION -- the previous version wrote the new record_mode exactly once
// per transition and immediately considered the job done (record_mode_
// hidden_override cleared unconditionally in the un-hide branch, before
// even checking whether the restore write actually landed). If that single
// obs_source_update() ever got lost or stomped by something else touching
// this filter's settings around the same time (its own properties dialog
// being open and saving stale state is the obvious candidate, though not
// confirmed), nothing would ever notice or retry -- it'd just sit silently
// wrong until the next real hide/show transition. Now it re-checks and
// re-applies every interval regardless, and only clears the override flag
// once the setting is actually confirmed to match, not on faith that the
// write succeeded.
static void update_hidden_record_mode(struct source_record_filter_context *context, obs_source_t *parent, float seconds)
{
	context->visibility_check_accum += seconds;
	if (context->visibility_check_accum < 0.5f)
		return;
	context->visibility_check_accum = 0.0f;

	const bool hidden = source_hidden_in_current_scene(parent);
	context->source_hidden_in_scene = hidden;

	// Keeps the filter's own eye icon in the Filters list in sync with
	// `hidden` too -- record_mode alone (below) already stops the actual
	// output, but left the icon looking permanently "on" regardless, which
	// read as this feature not doing anything at all. Same self-verifying
	// pattern as record_mode below (checked every interval, not just
	// applied once and trusted): confirmed live against libobs's own
	// tick_sources (obs-video.c) that every loaded source, filters
	// included, keeps getting ticked regardless of enabled state -- so a
	// disabled filter can still notice being shown again and re-enable
	// itself; it can't get stuck disabled.
	if (obs_source_enabled(context->source) == hidden)
		obs_source_set_enabled(context->source, !hidden);

	obs_data_t *current_settings = obs_source_get_settings(context->source);
	const long long current_mode = obs_data_get_int(current_settings, "record_mode");

	if (hidden) {
		if (!context->record_mode_hidden_override) {
			context->saved_record_mode = current_mode;
			context->record_mode_hidden_override = true;
		}
		if (current_mode != OUTPUT_MODE_NONE) {
			obs_data_t *update = obs_data_create();
			obs_data_set_int(update, "record_mode", OUTPUT_MODE_NONE);
			obs_source_update(context->source, update);
			obs_data_release(update);
		}
	} else if (context->record_mode_hidden_override) {
		if (current_mode != context->saved_record_mode) {
			obs_data_t *update = obs_data_create();
			obs_data_set_int(update, "record_mode", context->saved_record_mode);
			obs_source_update(context->source, update);
			obs_data_release(update);
		} else {
			context->record_mode_hidden_override = false;
		}
	}
	obs_data_release(current_settings);
}

// Percentage of frames dropped since the last check (not lifetime-cumulative
// -- see the struct fields' own comment), or -1 if this output isn't
// currently active, or if there isn't yet a full interval of new data to
// compute a rate from (first check after it started, or momentarily paused).
static float dropped_rate_since_last_check(obs_output_t *output, int *prev_total, int *prev_dropped)
{
	if (!output || !obs_output_active(output)) {
		*prev_total = 0;
		*prev_dropped = 0;
		return -1.0f;
	}
	int total = obs_output_get_total_frames(output);
	int dropped = obs_output_get_frames_dropped(output);
	int delta_total = total - *prev_total;
	int delta_dropped = dropped - *prev_dropped;
	*prev_total = total;
	*prev_dropped = dropped;
	if (delta_total <= 0)
		return -1.0f;
	return 100.0f * (float)delta_dropped / (float)delta_total;
}

// A couple percent of dropped frames is common/harmless jitter, not a real
// problem worth interrupting the user about.
#define ENCODER_OVERLOAD_THRESHOLD 5.0f

// Checks this filter's own active output AND the 3 main-program outputs
// (recording/streaming/replay buffer, via obs-frontend-api) for a real
// recent drop rate, and -- only if at least one crosses the threshold --
// emits a websocket vendor event naming specifically which one(s), so an
// external tool (Backtrack) can tell a user "it's the main stream" instead
// of just "something's overloaded". Shared hardware (e.g. one NVENC chip)
// means this filter's own output is never the only possible cause, and
// checking only it would be misleading in either direction.
static void check_encoder_overload(struct source_record_filter_context *context, float seconds)
{
	context->encoder_overload_check_accum += seconds;
	if (context->encoder_overload_check_accum < 2.0f)
		return;
	context->encoder_overload_check_accum = 0.0f;
	/* Deliberately NOT gated on `vendor` here anymore -- it used to return
	 * immediately if obs-websocket wasn't loaded, which meant this filter's
	 * own drop-rate diagnostics went completely dark (no OBS log line
	 * either) for anyone not running Backtrack/obs-websocket. The vendor
	 * event is still only emitted when vendor is available; local logging
	 * below always runs regardless, so `Encoding overloaded` can be
	 * root-caused from the OBS log alone. */

	obs_output_t *own_output = context->fileOutput && obs_output_active(context->fileOutput)     ? context->fileOutput
				    : context->replayOutput && obs_output_active(context->replayOutput) ? context->replayOutput
				    : context->streamOutput && obs_output_active(context->streamOutput) ? context->streamOutput
											      : NULL;
	float own_rate = dropped_rate_since_last_check(own_output, &context->own_prev_total, &context->own_prev_dropped);

	obs_output_t *main_rec = obs_frontend_get_recording_output();
	float rec_rate = dropped_rate_since_last_check(main_rec, &context->main_rec_prev_total, &context->main_rec_prev_dropped);
	if (main_rec)
		obs_output_release(main_rec);

	obs_output_t *main_stream = obs_frontend_get_streaming_output();
	float stream_rate =
		dropped_rate_since_last_check(main_stream, &context->main_stream_prev_total, &context->main_stream_prev_dropped);
	if (main_stream)
		obs_output_release(main_stream);

	obs_output_t *main_replay = obs_frontend_get_replay_buffer_output();
	float replay_rate =
		dropped_rate_since_last_check(main_replay, &context->main_replay_prev_total, &context->main_replay_prev_dropped);
	if (main_replay)
		obs_output_release(main_replay);

	bool own_bad = own_rate >= ENCODER_OVERLOAD_THRESHOLD;
	bool rec_bad = rec_rate >= ENCODER_OVERLOAD_THRESHOLD;
	bool stream_bad = stream_rate >= ENCODER_OVERLOAD_THRESHOLD;
	bool replay_bad = replay_rate >= ENCODER_OVERLOAD_THRESHOLD;

	if (!own_bad && !rec_bad && !stream_bad && !replay_bad)
		return;

	blog(LOG_WARNING,
	     "[Source Record] '%s': encoder overload (dropped frames last ~2s -- this filter: %.1f%%, "
	     "main recording: %.1f%%, main stream: %.1f%%, main replay buffer: %.1f%%)",
	     obs_source_get_name(context->source), own_rate, rec_rate, stream_rate, replay_rate);

	if (!vendor)
		return; // obs-websocket isn't installed/loaded -- nothing to emit to, logged above regardless

	obs_data_t *event_data = obs_data_create();
	obs_data_set_bool(event_data, "this_filter", own_bad);
	obs_data_set_bool(event_data, "main_recording", rec_bad);
	// Unlike recording/replay buffer (file outputs -- a drop there really is
	// encoder/disk-side lag), a streaming output's dropped frames can also
	// be network/bandwidth congestion, nothing to do with the encoder at
	// all. Reported as its own flag either way; the caller decides how to
	// word that distinction rather than this plugin asserting a cause it
	// can't actually distinguish.
	obs_data_set_bool(event_data, "main_stream", stream_bad);
	obs_data_set_bool(event_data, "main_replay_buffer", replay_bad);
	obs_source_t *parent = obs_filter_get_parent(context->source);
	if (parent)
		obs_data_set_string(event_data, "source", obs_source_get_name(parent));
	obs_data_set_string(event_data, "filter", obs_source_get_name(context->source));
	obs_websocket_vendor_emit_event(vendor, "encoder_overload", event_data);
	obs_data_release(event_data);
}

/* Whether this filter currently needs its own obs_view/video_output mix at
 * all. That mix (created via obs_view_add2 below) is not a passive data
 * holder -- once registered, libobs's single shared video thread renders a
 * full extra composited scene for it every single output frame for as long
 * as it exists (obs-video.c: output_frames() -> output_frame() ->
 * render_video() -> render_main_texture(), unconditionally, for every mix in
 * obs->video.mixes -- verified against the actual OBS 32.1.2 source). That
 * cost was previously paid for the filter's entire lifetime, the moment the
 * parent had a valid size, regardless of whether record/stream/replay was
 * even turned on. With several filters on a scene collection (this fork's
 * whole point -- one filter per source, each with its own up-to-4-overlay
 * composite) most of them idle at any moment, that meant every idle filter
 * was still fully rendering its own scene every frame on the one thread OBS
 * also uses for the main program output -- a real, avoidable contributor to
 * "Encoding overloaded" that has nothing to do with bitrate/resolution/fps.
 * Gate mix creation on actually needing it, and tear it down once nothing
 * needs it anymore, so an idle filter costs the video thread nothing. */
static bool filter_needs_video_pipeline(struct source_record_filter_context *context)
{
	/* Deliberately NOT including context->output_active here -- it's only
	 * cleared by two specific tick() branches (parent hidden/disabled, and
	 * resize/restart), not by source_record_filter_update()'s own direct
	 * stop-on-settings-change path (record_mode/stream_mode/replay_buffer
	 * flipped off while the source stays visible and enabled), so it can
	 * sit stuck true long after nothing is actually running. encoder_active
	 * is the reliable signal instead: obs_encoder_active() is a real
	 * ref-count internal to libobs (obs-encoder.c's add_connection/
	 * remove_connection, incremented on obs_encoder_start, decremented on
	 * obs_encoder_stop) that only goes false once every output still
	 * attached to this encoder -- file, stream, and replay buffer share the
	 * one encoder object -- has fully finished its own async drain
	 * (obs_output_end_data_capture's end_data_capture_thread), regardless
	 * of which of the three paths triggered the stop. */
	return context->record || context->stream || context->replayBuffer || context->starting_file_output ||
	       context->starting_stream_output || context->starting_replay_output ||
	       (context->encoder && obs_encoder_active(context->encoder));
}

static void source_record_filter_tick(void *data, float seconds)
{
	struct source_record_filter_context *context = data;
	if (context->closing)
		return;

	obs_source_t *parent = obs_filter_get_parent(context->source);
	if (!parent)
		return;

	if (obs_obj_is_private(parent)) {
		context->closing = true;
		return;
	}

	update_hidden_record_mode(context, parent, seconds);
	check_encoder_overload(context, seconds);

	if (context->enableHotkey == OBS_INVALID_HOTKEY_PAIR_ID)
		context->enableHotkey = obs_hotkey_pair_register_source(
			parent, "source_record.enable", obs_module_text("SourceRecordEnable"), "source_record.disable",
			obs_module_text("SourceRecordDisable"), source_record_enable_hotkey, source_record_disable_hotkey, context,
			context);

	if (context->pauseHotkeys == OBS_INVALID_HOTKEY_PAIR_ID)
		context->pauseHotkeys = obs_hotkey_pair_register_source(
			parent, "source_record.PauseRecording", obs_frontend_get_locale_string("Basic.Main.PauseRecording"),
			"source_record.UnpauseRecording", obs_frontend_get_locale_string("Basic.Main.UnpauseRecording"),
			source_record_pause_hotkey, source_record_unpause_hotkey, context, context);

	// Distinct from enableHotkey above -- Enable/Disable toggles the whole
	// filter (replay buffer AND recording, whichever record_mode/replay
	// settings resolve to), while this pair only ever touches record_mode,
	// same scope as the record_start/record_stop websocket vendor requests.
	if (context->recordHotkeys == OBS_INVALID_HOTKEY_PAIR_ID)
		context->recordHotkeys = obs_hotkey_pair_register_source(
			parent, "source_record.StartRecording", obs_module_text("SourceRecordStartRecording"),
			"source_record.StopRecording", obs_module_text("SourceRecordStopRecording"),
			source_record_start_record_hotkey, source_record_stop_record_hotkey, context, context);

	if (context->splitHotkey == OBS_INVALID_HOTKEY_ID)
		context->splitHotkey = obs_hotkey_register_source(parent, "source_record.SplitRecording",
								  obs_frontend_get_locale_string("Basic.Main.SplitFile"),
								  source_record_split_hotkey, context);

	if (context->chapterHotkey == OBS_INVALID_HOTKEY_ID)
		context->chapterHotkey = obs_hotkey_register_source(parent, "source_record.AddChapterMarker",
								    obs_frontend_get_locale_string("Basic.Main.AddChapterMarker"),
								    source_record_chapter_hotkey, context);

	uint32_t width = obs_source_get_width(parent);
	width += (width & 1);
	uint32_t height = obs_source_get_height(parent);
	height += (height & 1);
	/* Fix: never destroy the video_output while an encoder is still feeding
	 * from it. A size change on the parent (window capture / PipeWire on
	 * focus change) used to call obs_view_remove() immediately, freeing the
	 * video_output under a running encoder + replay buffer -> crash. */
	const bool size_changed = width && height && (context->width != width || context->height != height);
	const bool needs_pipeline = filter_needs_video_pipeline(context);

	if (width && height && needs_pipeline && !context->video_output) {
		struct obs_video_info ovi = {0};
		obs_get_video_info(&ovi);

		ovi.base_width = width;
		ovi.base_height = height;
		ovi.output_width = width;
		ovi.output_height = height;

		if (!context->view)
			context->view = obs_view_create();

		context->video_output = obs_view_add2(context->view, &ovi);
		if (context->video_output) {
			context->width = width;
			context->height = height;
			blog(LOG_INFO, "[Source Record] '%s': video pipeline started (%ux%u)",
			     obs_source_get_name(context->source), width, height);
		}
	} else if (!needs_pipeline && context->video_output) {
		/* Safe by the same invariant as the resize-swap branch below: only
		 * tear down once nothing (including a still-winding-down encoder)
		 * is using it. needs_pipeline already covers that, so this is just
		 * the mirror image of the creation branch above. */
		obs_view_remove(context->view);
		context->video_output = NULL;
		context->width = 0;
		context->height = 0;
		blog(LOG_INFO, "[Source Record] '%s': video pipeline stopped (idle -- nothing to record/stream/buffer)",
		     obs_source_get_name(context->source));
	} else if (size_changed && context->video_output) {
		if (context->output_active) {
			/* defer: let the restart branch stop the outputs first */
			context->restart = true;
		} else if (!context->encoder || !obs_encoder_active(context->encoder)) {
			/* outputs are down and the encoder is idle: safe to swap */
			struct obs_video_info ovi = {0};
			obs_get_video_info(&ovi);

			ovi.base_width = width;
			ovi.base_height = height;
			ovi.output_width = width;
			ovi.output_height = height;

			obs_view_remove(context->view);
			context->video_output = obs_view_add2(context->view, &ovi);
			if (context->video_output) {
				context->width = width;
				context->height = height;
				if (context->encoder)
					obs_encoder_set_video(context->encoder, context->video_output);
			}
		}
		/* else: encoder still winding down, retry on the next tick */
	}

	if (context->restart && context->output_active) {
		if (context->fileOutput) {
			struct stop_output *so = bmalloc(sizeof(struct stop_output));
			so->output = context->fileOutput;
			so->context = context;
			run_queued(force_stop_output_task, so);
			context->fileOutput = NULL;
		}
		if (context->streamOutput) {
			struct stop_output *so = bmalloc(sizeof(struct stop_output));
			so->output = context->streamOutput;
			so->context = context;
			run_queued(force_stop_output_task, so);
			context->streamOutput = NULL;
		}
		if (context->replayOutput) {
			struct stop_output *so = bmalloc(sizeof(struct stop_output));
			so->output = context->replayOutput;
			so->context = context;
			run_queued(force_stop_output_task, so);
			context->replayOutput = NULL;
		}
		context->output_active = false;
		context->restart = false;
		obs_source_dec_showing(obs_filter_get_parent(context->source));
	} else if (!context->output_active && obs_source_enabled(context->source) &&
		   (context->replayBuffer || context->record || context->stream)) {
		if (context->starting_file_output || context->starting_stream_output || context->starting_replay_output ||
		    !context->video_output || !width || !height)
			return;
		/* Fix: don't start new outputs (and reuse / re-point the encoder)
		 * while a previous output is still draining it -> use-after-free. */
		if (context->encoder && obs_encoder_active(context->encoder))
			return;
		obs_data_t *s = obs_source_get_settings(context->source);
		update_encoder(context, s);
		if (context->record || context->stream || context->replayBuffer) {
			obs_source_t *view_source = obs_view_get_source(context->view, SOURCE_CHANNEL);
			if (view_source != parent)
				obs_view_set_source(context->view, SOURCE_CHANNEL, parent);
			obs_source_release(view_source);
		}

		obs_source_t *background_source = obs_view_get_source(context->view, BACKGROUND_CHANNEL);
		if (!background_source) {
			background_source = obs_source_create_private("color_source", "Source Record Background", NULL);
			obs_view_set_source(context->view, BACKGROUND_CHANNEL, background_source);
		}
		obs_data_t *css = obs_source_get_settings(background_source);
		if (obs_data_get_int(css, "color") != obs_data_get_int(s, "backgroundColor") ||
		    obs_data_get_int(css, "width") != obs_source_get_width(parent) ||
		    obs_data_get_int(css, "height") != obs_source_get_height(parent)) {
			obs_data_set_int(css, "color", obs_data_get_int(s, "backgroundColor"));
			obs_data_set_int(css, "width", obs_source_get_width(parent));
			obs_data_set_int(css, "height", obs_source_get_height(parent));
			obs_source_update(background_source, css);
		}
		obs_data_release(css);
		obs_source_release(background_source);

		update_overlay_sources(context, s);

		if (context->record)
			start_file_output(context, s);
		if (context->stream)
			start_stream_output(context, s);
		if (context->replayBuffer)
			start_replay_output(context, s);
		obs_data_release(s);
	} else if (context->output_active && !obs_source_enabled(context->source)) {
		if (context->fileOutput) {
			struct stop_output *so = bmalloc(sizeof(struct stop_output));
			so->output = context->fileOutput;
			so->context = context;
			run_queued(force_stop_output_task, so);
			context->fileOutput = NULL;
		}
		if (context->streamOutput) {
			struct stop_output *so = bmalloc(sizeof(struct stop_output));
			so->output = context->streamOutput;
			so->context = context;
			run_queued(force_stop_output_task, so);
			context->streamOutput = NULL;
		}
		if (context->replayOutput) {
			struct stop_output *so = bmalloc(sizeof(struct stop_output));
			so->output = context->replayOutput;
			so->context = context;
			run_queued(force_stop_output_task, so);
			context->replayOutput = NULL;
		}
		context->output_active = false;
		obs_source_dec_showing(obs_filter_get_parent(context->source));
	}

	if (context->output_active && context->fileOutput && context->record_max_seconds) {
		int totalFrames = obs_output_get_total_frames(context->fileOutput);
		video_t *video = obs_output_video(context->fileOutput);
		uint64_t frameTimeNs = video_output_get_frame_time(video);
		long long msecs = util_mul_div64(totalFrames, frameTimeNs, 1000000ULL);
		if (msecs >= context->record_max_seconds * 1000) {
			obs_data_t *settings = obs_data_create();
			obs_data_set_int(settings, "record_mode", OUTPUT_MODE_NONE);
			obs_source_update(context->source, settings);
			obs_data_release(settings);
		}
	}
}

static void all_properties_changed(obs_properties_t *props, obs_data_t *settings)
{
	obs_property_t *property = obs_properties_first(props);
	while (property) {
		if (obs_property_get_type(property) == OBS_PROPERTY_GROUP) {
			all_properties_changed(obs_property_group_content(property), settings);
		} else {
			obs_property_modified(property, settings);
		}
		obs_property_next(&property);
	}
}

static bool encoder_changed(void *data, obs_properties_t *props, obs_property_t *property, obs_data_t *settings)
{
	UNUSED_PARAMETER(data);
	UNUSED_PARAMETER(property);
	obs_properties_remove_by_name(props, "encoder_group");
	obs_properties_remove_by_name(props, "audio_encoder_group");
	bool visible = obs_property_visible(obs_properties_get(props, "others"));
	obs_properties_remove_by_name(props, "others");
	obs_properties_remove_by_name(props, "plugin_info");
	set_encoder_defaults(settings);
	const char *enc_id = get_encoder_id(settings);
	obs_properties_t *enc_props = obs_get_encoder_properties(enc_id);
	if (enc_props) {
		all_properties_changed(enc_props, settings);
		obs_properties_add_group(props, "encoder_group", obs_encoder_get_display_name(enc_id), OBS_GROUP_NORMAL, enc_props);
	}

	enc_id = obs_data_get_string(settings, "audio_encoder");
	if (!enc_id || !strlen(enc_id))
		enc_id = "ffmpeg_aac";
	enc_props = obs_get_encoder_properties(enc_id);
	if (enc_props) {
		all_properties_changed(enc_props, settings);
		const char *name = obs_encoder_get_display_name(enc_id);
		if (!name || !strlen(name))
			name = obs_module_text("AudioEncoder");
		obs_property_t *b = obs_properties_get(enc_props, "bitrate");
		if (b) {
			obs_property_int_set_suffix(obs_properties_add_int(enc_props, "audio_bitrate", obs_property_description(b),
									   obs_property_int_min(b), obs_property_int_max(b),
									   obs_property_int_step(b)),
						    obs_property_int_suffix(b));
			obs_properties_remove_by_name(enc_props, "bitrate");
		}
		obs_properties_add_group(props, "audio_encoder_group", name, OBS_GROUP_NORMAL, enc_props);
	}

	obs_property_t *p = obs_properties_add_text(props, "others", obs_module_text("OtherSourceRecords"), OBS_TEXT_INFO);
	obs_property_set_visible(p, visible);
	obs_properties_add_text(
		props, "plugin_info",
		"<a href=\"https://obsproject.com/forum/resources/source-record.1285/\">Source Record</a> (" PROJECT_VERSION
		") by <a href=\"https://www.exeldro.com\">Exeldro</a>",
		OBS_TEXT_INFO);
	return true;
}

static bool audio_track_changed(void *data, obs_properties_t *props, obs_property_t *property, obs_data_t *settings)
{
	UNUSED_PARAMETER(data);
	UNUSED_PARAMETER(property);
	const bool custom = obs_data_get_int(settings, "audio_track") == AUDIO_TRACK_CUSTOM;
	char prop_name[16];
	for (int i = 1; i <= MAX_AUDIO_MIXES; i++) {
		snprintf(prop_name, sizeof(prop_name), "audio_track_%d", i);
		obs_property_t *track_check = obs_properties_get(props, prop_name);
		if (track_check)
			obs_property_set_visible(track_check, custom);
	}
	return true;
}

static bool list_add_audio_sources(void *data, obs_source_t *source)
{
	obs_property_t *p = data;
	const uint32_t flags = obs_source_get_output_flags(source);
	if ((flags & OBS_SOURCE_COMPOSITE) != 0) {
		obs_property_list_add_string(p, obs_source_get_name(source), obs_source_get_name(source));
	} else if ((flags & OBS_SOURCE_AUDIO) != 0) {
		obs_property_list_add_string(p, obs_source_get_name(source), obs_source_get_name(source));
	}
	return true;
}

static bool list_add_video_sources(void *data, obs_source_t *source)
{
	obs_property_t *p = data;
	const uint32_t flags = obs_source_get_output_flags(source);
	if ((flags & OBS_SOURCE_VIDEO) != 0)
		obs_property_list_add_string(p, obs_source_get_name(source), obs_source_get_name(source));
	return true;
}

static bool list_add_scene_item(obs_scene_t *scene, obs_sceneitem_t *item, void *data)
{
	UNUSED_PARAMETER(scene);
	obs_property_t *p = data;
	obs_source_t *source = obs_sceneitem_get_source(item);
	if (!source)
		return true;
	const uint32_t flags = obs_source_get_output_flags(source);
	if ((flags & OBS_SOURCE_VIDEO) != 0)
		obs_property_list_add_string(p, obs_source_get_name(source), obs_source_get_name(source));
	return true;
}

static bool overlay_scene_changed(void *data, obs_properties_t *props, obs_property_t *property, obs_data_t *settings)
{
	UNUSED_PARAMETER(data);
	UNUSED_PARAMETER(property);
	const char *scene_name = obs_data_get_string(settings, "overlay_scene");
	obs_source_t *scene_source = (scene_name && strlen(scene_name)) ? obs_get_source_by_name(scene_name) : NULL;
	obs_scene_t *scene = scene_source ? obs_scene_from_source(scene_source) : NULL;

	char prop_name[24];
	for (int i = 1; i <= MAX_OVERLAY_SOURCES; i++) {
		snprintf(prop_name, sizeof(prop_name), "overlay_source_%d", i);
		obs_property_t *p = obs_properties_get(props, prop_name);
		if (!p)
			continue;
		obs_property_list_clear(p);
		obs_property_list_add_string(p, obs_module_text("None"), "");
		if (scene)
			obs_scene_enum_items(scene, list_add_scene_item, p);
	}

	if (scene_source)
		obs_source_release(scene_source);
	return true;
}

static bool source_record_split_button(obs_properties_t *props, obs_property_t *property, void *data)
{
	UNUSED_PARAMETER(props);
	UNUSED_PARAMETER(property);
	struct source_record_filter_context *context = data;
	if (!context->fileOutput)
		return false;
	proc_handler_t *ph = obs_output_get_proc_handler(context->fileOutput);
	struct calldata cd;
	calldata_init(&cd);
	proc_handler_call(ph, "split_file", &cd);
	calldata_free(&cd);
	return true;
}

bool output_exists(const char *id)
{
	const char *output_id;
	size_t i = 0;
	while (obs_enum_output_types(i++, &output_id)) {
		if (strcmp(id, output_id) == 0)
			return true;
	}
	return false;
}

static obs_properties_t *source_record_filter_properties(void *data)
{
	obs_properties_t *props = obs_properties_create();

	obs_properties_t *record = obs_properties_create();

	obs_property_t *p = obs_properties_add_list(record, "record_mode", obs_module_text("RecordMode"), OBS_COMBO_TYPE_LIST,
						    OBS_COMBO_FORMAT_INT);

	obs_property_list_add_int(p, obs_module_text("None"), OUTPUT_MODE_NONE);
	obs_property_list_add_int(p, obs_module_text("Always"), OUTPUT_MODE_ALWAYS);
	obs_property_list_add_int(p, obs_module_text("Streaming"), OUTPUT_MODE_STREAMING);
	obs_property_list_add_int(p, obs_module_text("Recording"), OUTPUT_MODE_RECORDING);
	obs_property_list_add_int(p, obs_module_text("StreamingOrRecording"), OUTPUT_MODE_STREAMING_OR_RECORDING);
	obs_property_list_add_int(p, obs_module_text("VirtualCamera"), OUTPUT_MODE_VIRTUAL_CAMERA);

	obs_properties_add_path(record, "path", obs_module_text("Path"), OBS_PATH_DIRECTORY, NULL, NULL);
	obs_properties_add_text(record, "filename_formatting", obs_module_text("FilenameFormatting"), OBS_TEXT_DEFAULT);
	p = obs_properties_add_list(record, "rec_format", obs_module_text("RecFormat"), OBS_COMBO_TYPE_LIST,
				    OBS_COMBO_FORMAT_STRING);
	obs_property_list_add_string(p, "flv", "flv");
	obs_property_list_add_string(p, "mp4", "mp4");
	if (output_exists("mp4_output"))
		obs_property_list_add_string(p, "hybrid_mp4", "hybrid_mp4");
	if (output_exists("mov_output"))
		obs_property_list_add_string(p, "hybrid_mov", "hybrid_mov");
	obs_property_list_add_string(p, "fragmented_mp4", "fragmented_mp4");
	obs_property_list_add_string(p, "fragmented_mov", "fragmented_mov");
	obs_property_list_add_string(p, "mov", "mov");
	obs_property_list_add_string(p, "mkv", "mkv");
	obs_property_list_add_string(p, "ts", "ts");
	obs_property_list_add_string(p, "m3u8", "m3u8");

	obs_properties_t *split_file = obs_properties_create();
	p = obs_properties_add_int(split_file, "max_time_sec",
				   obs_frontend_get_locale_string("Basic.Settings.Output.SplitFile.Time"), 0, 31536000, 1);
	obs_property_int_set_suffix(p, " s");
	p = obs_properties_add_int(split_file, "max_size_mb",
				   obs_frontend_get_locale_string("Basic.Settings.Output.SplitFile.Size"), 0, 1073741824, 1);
	obs_property_int_set_suffix(p, " MB");
	obs_properties_add_button(split_file, "split_file_now", obs_frontend_get_locale_string("Basic.Main.SplitFile"),
				 source_record_split_button);
	obs_properties_add_group(record, "split_file", obs_frontend_get_locale_string("Basic.Settings.Output.EnableSplitFile"),
				 OBS_GROUP_CHECKABLE, split_file);

	obs_properties_add_int(record, "record_max_seconds", obs_module_text("MaxSeconds"), 0, 31536000, 1);

	obs_properties_add_group(props, "record", obs_module_text("Record"), OBS_GROUP_NORMAL, record);

	obs_properties_t *replay = obs_properties_create();

	p = obs_properties_add_int(replay, "replay_duration", obs_module_text("Duration"), 1, 10000, 1);
	obs_property_int_set_suffix(p, "s");

	obs_properties_add_text(replay, "replay_filename_formatting", obs_module_text("FilenameFormatting"), OBS_TEXT_DEFAULT);

	obs_properties_add_group(props, "replay_buffer", obs_module_text("ReplayBuffer"), OBS_GROUP_CHECKABLE, replay);

	obs_properties_t *stream = obs_properties_create();

	p = obs_properties_add_list(stream, "stream_mode", obs_module_text("StreamMode"), OBS_COMBO_TYPE_LIST,
				    OBS_COMBO_FORMAT_INT);

	obs_property_list_add_int(p, obs_module_text("None"), OUTPUT_MODE_NONE);
	obs_property_list_add_int(p, obs_module_text("Always"), OUTPUT_MODE_ALWAYS);
	obs_property_list_add_int(p, obs_module_text("Streaming"), OUTPUT_MODE_STREAMING);
	obs_property_list_add_int(p, obs_module_text("Recording"), OUTPUT_MODE_RECORDING);
	obs_property_list_add_int(p, obs_module_text("StreamingOrRecording"), OUTPUT_MODE_STREAMING_OR_RECORDING);
	obs_property_list_add_int(p, obs_module_text("VirtualCamera"), OUTPUT_MODE_VIRTUAL_CAMERA);

	obs_properties_add_text(stream, "server", obs_module_text("Server"), OBS_TEXT_DEFAULT);
	obs_properties_add_text(stream, "key", obs_module_text("Key"), OBS_TEXT_PASSWORD);

	obs_properties_add_group(props, "stream", obs_module_text("Stream"), OBS_GROUP_NORMAL, stream);

	obs_properties_t *background = obs_properties_create();

	obs_properties_add_color(background, "backgroundColor", obs_module_text("BackgroundColor"));

	obs_properties_add_group(props, "background", obs_module_text("Background"), OBS_GROUP_NORMAL, background);

	obs_properties_t *overlays = obs_properties_create();
	p = obs_properties_add_list(overlays, "overlay_scene", obs_module_text("OverlayScene"), OBS_COMBO_TYPE_LIST,
				    OBS_COMBO_FORMAT_STRING);
	obs_property_list_add_string(p, obs_module_text("None"), "");
	obs_enum_scenes(list_add_video_sources, p);
	obs_property_set_long_description(p, obs_module_text("OverlaySceneNote"));
	obs_property_set_modified_callback2(p, overlay_scene_changed, data);

	for (int i = 1; i <= MAX_OVERLAY_SOURCES; i++) {
		char prop_name[24];
		snprintf(prop_name, sizeof(prop_name), "overlay_source_%d", i);
		char buffer[64];
		snprintf(buffer, 64, "%s %i", obs_module_text("Overlay"), i);
		obs_property_t *overlay_list =
			obs_properties_add_list(overlays, prop_name, buffer, OBS_COMBO_TYPE_EDITABLE, OBS_COMBO_FORMAT_STRING);
		obs_property_list_add_string(overlay_list, obs_module_text("None"), "");
	}
	obs_properties_add_group(props, "overlays", obs_module_text("AdditionalSources"), OBS_GROUP_NORMAL, overlays);

	obs_properties_t *audio = obs_properties_create();

	p = obs_properties_add_list(audio, "audio_track", obs_module_text("AudioTrack"), OBS_COMBO_TYPE_LIST, OBS_COMBO_FORMAT_INT);
	obs_property_list_add_int(p, obs_module_text("None"), 0);
	obs_property_list_add_int(p, obs_module_text("All"), -1);
	const char *track = obs_module_text("Track");
	for (int i = 1; i <= MAX_AUDIO_MIXES; i++) {
		char buffer[64];
		snprintf(buffer, 64, "%s %i", track, i);
		obs_property_list_add_int(p, buffer, i);
	}
	obs_property_list_add_int(p, obs_module_text("Custom"), AUDIO_TRACK_CUSTOM);
	obs_property_set_modified_callback2(p, audio_track_changed, data);

	for (int i = 1; i <= MAX_AUDIO_MIXES; i++) {
		char prop_name[16];
		snprintf(prop_name, sizeof(prop_name), "audio_track_%d", i);
		char buffer[64];
		snprintf(buffer, 64, "%s %i", track, i);
		obs_property_t *track_check = obs_properties_add_bool(audio, prop_name, buffer);
		obs_property_set_visible(track_check, false);
	}

	p = obs_properties_add_list(audio, "audio_source", obs_module_text("Source"), OBS_COMBO_TYPE_EDITABLE,
				    OBS_COMBO_FORMAT_STRING);
	obs_enum_sources(list_add_audio_sources, p);
	obs_enum_scenes(list_add_audio_sources, p);

	obs_properties_add_group(props, "different_audio", obs_module_text("DifferentAudio"), OBS_GROUP_CHECKABLE, audio);

	obs_properties_t *scale = obs_properties_create();
	p = obs_properties_add_list(scale, "resolution", obs_module_text("Resolution"), OBS_COMBO_TYPE_EDITABLE,
				    OBS_COMBO_FORMAT_STRING);

	obs_property_list_add_string(p, "640x480", "640x480");
	obs_property_list_add_string(p, "800x600", "800x600");
	obs_property_list_add_string(p, "1280x720", "1280x720");
	obs_property_list_add_string(p, "1920x1080", "1920x1080");
	obs_property_list_add_string(p, "2560x1440", "2560x1440");

	if (obs_encoder_set_gpu_scale_type_func) {
		p = obs_properties_add_list(scale, "scale_type", obs_module_text("ScaleType"), OBS_COMBO_TYPE_LIST,
					    OBS_COMBO_FORMAT_INT);
		obs_property_list_add_int(p, obs_frontend_get_locale_string("Basic.Settings.Video.DownscaleFilter.Bilinear"),
					  OBS_SCALE_BILINEAR);
		obs_property_list_add_int(p, obs_frontend_get_locale_string("Basic.Settings.Video.DownscaleFilter.Area"),
					  OBS_SCALE_AREA);
		obs_property_list_add_int(p, obs_frontend_get_locale_string("Basic.Settings.Video.DownscaleFilter.Bicubic"),
					  OBS_SCALE_BICUBIC);
		obs_property_list_add_int(p, obs_frontend_get_locale_string("Basic.Settings.Video.DownscaleFilter.Lanczos"),
					  OBS_SCALE_LANCZOS);
	}

	obs_properties_add_group(props, "scale", obs_module_text("Scale"), OBS_GROUP_CHECKABLE, scale);

	if (obs_encoder_set_frame_rate_divisor_func) {
		p = obs_properties_add_list(props, "frame_rate_divisor", obs_module_text("Framerate"), OBS_COMBO_TYPE_LIST,
					    OBS_COMBO_FORMAT_INT);
		struct obs_video_info ovi;
		obs_get_video_info(&ovi);
		float fps = ovi.fps_den > 0 ? (float)ovi.fps_num / (float)ovi.fps_den : 0.0f;
		struct dstr str;
		dstr_init(&str);
		dstr_printf(&str, "%.2f fps", fps);
		obs_property_list_add_int(p, str.array, 0);
		for (int i = 2; i <= 10; i++) {
			dstr_printf(&str, "%.2f fps (/%d)", fps / (float)i, i);
			obs_property_list_add_int(p, str.array, i);
		}
		dstr_free(&str);
	}

	p = obs_properties_add_list(props, "encoder", obs_module_text("VideoEncoder"), OBS_COMBO_TYPE_LIST,
				    OBS_COMBO_FORMAT_STRING);
	obs_property_t *audio_encoder = obs_properties_add_list(props, "audio_encoder", obs_module_text("AudioEncoder"),
								OBS_COMBO_TYPE_LIST, OBS_COMBO_FORMAT_STRING);

	obs_property_list_add_string(p, obs_module_text("Software"), "x264");
	if (EncoderAvailable("obs_qsv11"))
		obs_property_list_add_string(p, obs_module_text("QSV.H264"), "qsv");
	if (EncoderAvailable("obs_qsv11_av1"))
		obs_property_list_add_string(p, obs_module_text("QSV.AV1"), "qsv_av1");
	if (EncoderAvailable("ffmpeg_nvenc") || EncoderAvailable("jim_nvenc") || EncoderAvailable("obs_nvenc_h264_tex"))
		obs_property_list_add_string(p, obs_module_text("NVENC.H264"), "nvenc");
	if (EncoderAvailable("jim_av1_nvenc") || EncoderAvailable("obs_nvenc_av1_tex"))
		obs_property_list_add_string(p, obs_module_text("NVENC.AV1"), "nvenc_av1");
	if (EncoderAvailable("h265_texture_amf"))
		obs_property_list_add_string(p, obs_module_text("AMD.HEVC"), "amd_hevc");
	if (EncoderAvailable("ffmpeg_hevc_nvenc") || EncoderAvailable("obs_nvenc_hevc_tex"))
		obs_property_list_add_string(p, obs_module_text("NVENC.HEVC"), "nvenc_hevc");
	if (EncoderAvailable("h264_texture_amf"))
		obs_property_list_add_string(p, obs_module_text("AMD.H264"), "amd");
	if (EncoderAvailable("av1_texture_amf"))
		obs_property_list_add_string(p, obs_module_text("AMD.AV1"), "amd_av1");
	if (EncoderAvailable("com.apple.videotoolbox.videoencoder.ave.avc"))
		obs_property_list_add_string(p, obs_module_text("Apple.H264"), "apple_h264");
	if (EncoderAvailable("com.apple.videotoolbox.videoencoder.ave.hevc"))
		obs_property_list_add_string(p, obs_module_text("Apple.HEVC"), "apple_hevc");

	const char *enc_id = NULL;
	size_t i = 0;
	while (obs_enum_encoder_types(i++, &enc_id)) {
		const uint32_t caps = obs_get_encoder_caps(enc_id);
		if ((caps & (OBS_ENCODER_CAP_DEPRECATED | OBS_ENCODER_CAP_INTERNAL)) != 0)
			continue;
		const char *name = obs_encoder_get_display_name(enc_id);
		if (obs_get_encoder_type(enc_id) == OBS_ENCODER_VIDEO) {
			obs_property_list_add_string(p, name, enc_id);
		} else {
			obs_property_list_add_string(audio_encoder, name, enc_id);
		}
	}
	obs_property_set_modified_callback2(p, encoder_changed, data);
	obs_property_set_modified_callback2(audio_encoder, encoder_changed, data);

	obs_properties_add_group(props, "encoder_group", obs_module_text("VideoEncoder"), OBS_GROUP_NORMAL,
				 obs_properties_create());
	obs_properties_add_group(props, "audio_encoder_group", obs_module_text("AudioEncoder"), OBS_GROUP_NORMAL,
				 obs_properties_create());

	p = obs_properties_add_text(props, "others", obs_module_text("OtherSourceRecords"), OBS_TEXT_INFO);
	if (data) {
		struct source_record_filter_context *context = data;
		struct dstr sources_text;
		dstr_init(&sources_text);
		for (size_t i = 0; i < source_record_filters.num; i++) {
			if (source_record_filters.array[i] == context->source)
				continue;
			if (sources_text.len)
				dstr_cat(&sources_text, "\n");
			obs_source_t *parent = obs_filter_get_parent(source_record_filters.array[i]);
			if (parent) {
				dstr_cat(&sources_text, obs_source_get_name(parent));
				dstr_cat(&sources_text, " - ");
			}
			dstr_cat(&sources_text, obs_source_get_name(source_record_filters.array[i]));
		}
		if (sources_text.len > 0) {
			obs_data_t *settings = obs_source_get_settings(context->source);
			obs_data_set_string(settings, "others", sources_text.array);
			obs_data_release(settings);
			obs_property_set_visible(p, true);
		} else {
			obs_property_set_visible(p, false);
		}
		dstr_free(&sources_text);
	} else {
		obs_property_set_visible(p, false);
	}

	obs_properties_add_text(
		props, "plugin_info",
		"<a href=\"https://obsproject.com/forum/resources/source-record.1285/\">Source Record</a> (" PROJECT_VERSION
		") by <a href=\"https://www.exeldro.com\">Exeldro</a>",
		OBS_TEXT_INFO);
	return props;
}

static void source_record_filter_render(void *data, gs_effect_t *effect)
{
	UNUSED_PARAMETER(effect);
	struct source_record_filter_context *context = data;
	obs_source_skip_video_filter(context->source);
}

static void source_record_filter_filter_remove(void *data, obs_source_t *parent)
{
	UNUSED_PARAMETER(parent);
	struct source_record_filter_context *context = data;
	context->closing = true;
	stop_output_sync(context, context->fileOutput);
	stop_output_sync(context, context->streamOutput);
	stop_output_sync(context, context->replayOutput);
	obs_frontend_remove_event_callback(frontend_event, context);
}

struct obs_source_info source_record_filter_info = {
	.id = "source_record_filter",
	.type = OBS_SOURCE_TYPE_FILTER,
	.output_flags = OBS_SOURCE_VIDEO,
	.get_name = source_record_filter_get_name,
	.create = source_record_filter_create,
	.destroy = source_record_filter_destroy,
	.update = source_record_filter_update,
	.load = source_record_filter_update,
	.save = source_record_filter_save,
	.get_defaults = source_record_filter_defaults,
	.video_tick = source_record_filter_tick,
	.get_properties = source_record_filter_properties,
	.filter_remove = source_record_filter_filter_remove,
	.video_render = source_record_filter_render,
};

OBS_DECLARE_MODULE()
OBS_MODULE_USE_DEFAULT_LOCALE("source-record", "en-US")
MODULE_EXPORT const char *obs_module_description(void)
{
	return "Source Record Filter";
}

static void find_filter(obs_source_t *parent, obs_source_t *child, void *param)
{
	UNUSED_PARAMETER(parent);
	const char *id = obs_source_get_unversioned_id(child);
	if (strcmp(id, "source_record_filter") != 0)
		return;
	obs_source_t **filter = param;
	*filter = child;
}

static void find_source_by_filter(obs_source_t *parent, obs_source_t *child, void *param)
{
	if (strcmp(obs_source_get_unversioned_id(child), "source_record_filter") != 0)
		return;

	DARRAY(obs_source_t *) *sources = param;
	darray_push_back(sizeof(obs_source_t *), &sources->da, &parent);
}

static bool find_source(void *data, obs_source_t *source)
{
	obs_source_enum_filters(source, find_source_by_filter, data);
	return true;
}

obs_source_t *get_source_record_filter(obs_source_t *source, obs_data_t *request_data, obs_data_t *response_data, bool create)
{
	const char *filter_name = obs_data_get_string(request_data, "filter");
	obs_source_t *filter = NULL;
	config_t *config = obs_frontend_get_profile_config();
	if (strlen(filter_name)) {
		filter = obs_source_get_filter_by_name(source, filter_name);
		if (!filter) {
			if (response_data)
				obs_data_set_string(response_data, "error", "filter not found");
			return NULL;
		}
		if (strcmp(obs_source_get_unversioned_id(filter), "source_record_filter") != 0) {
			if (response_data)
				obs_data_set_string(response_data, "error", "filter is not source record filter");
			obs_source_release(filter);
			return NULL;
		}
		struct source_record_filter_context *context = obs_obj_get_data(filter);
		if (context && context->output_active && create) {
			context->restart = true;
		}
	} else {
		obs_source_enum_filters(source, find_filter, &filter);
		filter = obs_source_get_ref(filter);
		if (!filter) {
			if (!create) {
				if (response_data)
					obs_data_set_string(response_data, "error", "failed to find filter");
				return NULL;
			}

			const char *filename = obs_data_get_string(request_data, "filename");
			if (!strlen(filename)) {
				filename = config_get_string(config, "Output", "FilenameFormatting");
			}
			obs_data_t *settings = obs_data_create();
			obs_data_set_bool(settings, "remove_after_record", true);
			char *filter_name = os_generate_formatted_filename(NULL, true, filename);
			filter = obs_source_get_filter_by_name(source, filter_name);
			if (!filter) {
				filter = obs_source_create("source_record_filter", filter_name, settings, NULL);
			} else if (strcmp(obs_source_get_unversioned_id(filter), "source_record_filter") != 0) {
				if (response_data)
					obs_data_set_string(response_data, "error", "filter is not source record filter");
				obs_source_release(filter);
				bfree(filter_name);
				obs_data_release(settings);
				return NULL;
			} else {
				struct source_record_filter_context *context = obs_obj_get_data(filter);
				if (context && context->output_active && create) {
					context->restart = true;
				}
			}
			bfree(filter_name);
			obs_data_release(settings);
			if (!filter) {
				if (response_data)
					obs_data_set_string(response_data, "error", "failed to create filter");
				return NULL;
			}
			obs_source_filter_add(source, filter);
		}
	}
	if (!obs_source_enabled(filter))
		obs_source_set_enabled(filter, true);
	return filter;
}

static bool start_record_source(obs_source_t *source, obs_data_t *request_data, obs_data_t *response_data)
{
	obs_source_t *filter = get_source_record_filter(source, request_data, response_data, true);
	if (!filter)
		return false;
	obs_data_t *settings = obs_source_get_settings(filter);
	const char *filename = obs_data_get_string(request_data, "filename");
	struct source_record_filter_context *context = obs_obj_get_data(filter);
	if (context && context->output_active) {
		if (strlen(filename)) {
			if (strstr(filename, "%") || strcmp(filename, obs_data_get_string(settings, "filename_formatting")) != 0) {
				context->restart = true;
			}
		} else if (strstr(obs_data_get_string(settings, "filename_formatting"), "%")) {
			context->restart = true;
		}
	}

	if (strlen(filename))
		obs_data_set_string(settings, "filename_formatting", filename);
	if (obs_data_has_user_value(request_data, "max_seconds"))
		obs_data_set_int(settings, "record_max_seconds", obs_data_get_int(request_data, "max_seconds"));
	obs_data_set_int(settings, "record_mode", OUTPUT_MODE_ALWAYS);

	obs_source_update(filter, settings);
	obs_data_release(settings);

	obs_source_release(filter);
	return true;
}

static bool pause_record_source(obs_source_t *source, obs_data_t *request_data, obs_data_t *response_data)
{
	obs_source_t *filter = get_source_record_filter(source, request_data, response_data, false);
	if (!filter)
		return false;

	/* context belongs to filter -- read it before releasing our ref, not
	 * after (a release-then-use ordering bug: this vendor call can race an
	 * async filter removal on another thread, in which case this ref could
	 * be the last one and releasing it first would free context out from
	 * under the dereference below). */
	struct source_record_filter_context *context = obs_obj_get_data(filter);
	if (!context->fileOutput) {
		obs_source_release(filter);
		return false;
	}
	obs_output_pause(context->fileOutput, true);
	obs_source_release(filter);
	return true;
}

static bool unpause_record_source(obs_source_t *source, obs_data_t *request_data, obs_data_t *response_data)
{
	obs_source_t *filter = get_source_record_filter(source, request_data, response_data, false);
	if (!filter)
		return false;

	/* See pause_record_source's comment above -- same release-after-use fix. */
	struct source_record_filter_context *context = obs_obj_get_data(filter);
	if (!context->fileOutput) {
		obs_source_release(filter);
		return false;
	}
	obs_output_pause(context->fileOutput, false);
	obs_source_release(filter);
	return true;
}

static bool split_record_source(obs_source_t *source, obs_data_t *request_data, obs_data_t *response_data)
{
	obs_source_t *filter = get_source_record_filter(source, request_data, response_data, false);
	if (!filter)
		return false;

	/* See pause_record_source's comment above -- same release-after-use fix. */
	struct source_record_filter_context *context = obs_obj_get_data(filter);
	if (!context->fileOutput) {
		obs_source_release(filter);
		return false;
	}
	proc_handler_t *ph = obs_output_get_proc_handler(context->fileOutput);
	struct calldata cd;
	calldata_init(&cd);
	bool ok = proc_handler_call(ph, "split_file", &cd);
	calldata_free(&cd);
	obs_source_release(filter);
	return ok;
}

static bool add_chapter_record_source(obs_source_t *source, obs_data_t *request_data, obs_data_t *response_data)
{
	obs_source_t *filter = get_source_record_filter(source, request_data, response_data, false);
	if (!filter)
		return false;

	/* See pause_record_source's comment above -- same release-after-use fix. */
	struct source_record_filter_context *context = obs_obj_get_data(filter);
	if (!context->fileOutput) {
		obs_source_release(filter);
		return false;
	}
	proc_handler_t *ph = obs_output_get_proc_handler(context->fileOutput);
	struct calldata cd;
	calldata_init(&cd);
	calldata_set_string(&cd, "chapter_name", obs_data_get_string(request_data, "chapter_name"));
	bool ok = proc_handler_call(ph, "add_chapter", &cd);
	calldata_free(&cd);
	obs_source_release(filter);
	return ok;
}

static bool stop_record_source(obs_source_t *source, obs_data_t *request_data, obs_data_t *response_data)
{
	obs_source_t *filter = get_source_record_filter(source, request_data, response_data, false);
	if (!filter)
		return false;

	obs_data_t *settings = obs_data_create();
	obs_data_set_int(settings, "record_mode", OUTPUT_MODE_NONE);
	obs_source_update(filter, settings);
	obs_data_release(settings);
	obs_source_release(filter);
	return true;
}

static void websocket_start_record(obs_data_t *request_data, obs_data_t *response_data, void *param)
{
	UNUSED_PARAMETER(param);
	const char *source_name = obs_data_get_string(request_data, "source");
	bool success = true;
	if (strlen(source_name)) {
		obs_source_t *source = obs_get_source_by_name(source_name);
		if (!source) {
			obs_data_set_string(response_data, "error", "source not found");
			obs_data_set_bool(response_data, "success", false);
			return;
		}
		if (obs_data_get_bool(request_data, "stop_existing"))
			stop_record_source(source, request_data, NULL);
		success = start_record_source(source, request_data, response_data);
		obs_source_release(source);
	} else {
		DARRAY(obs_source_t *) sources = {0};
		obs_enum_sources(find_source, &sources);
		obs_enum_scenes(find_source, &sources);
		if (!sources.num) {
			obs_data_set_string(response_data, "error", "no source found");
			obs_data_set_bool(response_data, "success", false);
			return;
		}
		for (size_t i = 0; i < sources.num; i++) {
			success = start_record_source(sources.array[i], request_data, response_data) && success;
		}
		da_free(sources);
	}
	obs_data_set_bool(response_data, "success", success);
}

static void websocket_pause_record(obs_data_t *request_data, obs_data_t *response_data, void *param)
{
	UNUSED_PARAMETER(param);
	const char *source_name = obs_data_get_string(request_data, "source");
	bool success = true;
	if (strlen(source_name)) {
		obs_source_t *source = obs_get_source_by_name(source_name);
		if (!source) {
			obs_data_set_string(response_data, "error", "source not found");
			obs_data_set_bool(response_data, "success", false);
			return;
		}
		success = pause_record_source(source, request_data, response_data);
		obs_source_release(source);
	} else {
		DARRAY(obs_source_t *) sources = {0};
		obs_enum_sources(find_source, &sources);
		obs_enum_scenes(find_source, &sources);
		if (!sources.num) {
			obs_data_set_string(response_data, "error", "no source found");
			obs_data_set_bool(response_data, "success", false);
			return;
		}
		for (size_t i = 0; i < sources.num; i++) {
			success = pause_record_source(sources.array[i], request_data, response_data) && success;
		}
		da_free(sources);
	}
	obs_data_set_bool(response_data, "success", success);
}

static void websocket_unpause_record(obs_data_t *request_data, obs_data_t *response_data, void *param)
{
	UNUSED_PARAMETER(param);
	const char *source_name = obs_data_get_string(request_data, "source");
	bool success = true;
	if (strlen(source_name)) {
		obs_source_t *source = obs_get_source_by_name(source_name);
		if (!source) {
			obs_data_set_string(response_data, "error", "source not found");
			obs_data_set_bool(response_data, "success", false);
			return;
		}
		success = unpause_record_source(source, request_data, response_data);
		obs_source_release(source);
	} else {
		DARRAY(obs_source_t *) sources = {0};
		obs_enum_sources(find_source, &sources);
		obs_enum_scenes(find_source, &sources);
		if (!sources.num) {
			obs_data_set_string(response_data, "error", "no source found");
			obs_data_set_bool(response_data, "success", false);
			return;
		}
		for (size_t i = 0; i < sources.num; i++) {
			success = unpause_record_source(sources.array[i], request_data, response_data) && success;
		}
		da_free(sources);
	}
	obs_data_set_bool(response_data, "success", success);
}

static void websocket_split_record(obs_data_t *request_data, obs_data_t *response_data, void *param)
{
	UNUSED_PARAMETER(param);
	const char *source_name = obs_data_get_string(request_data, "source");
	bool success = true;
	if (strlen(source_name)) {
		obs_source_t *source = obs_get_source_by_name(source_name);
		if (!source) {
			obs_data_set_string(response_data, "error", "source not found");
			obs_data_set_bool(response_data, "success", false);
			return;
		}
		success = split_record_source(source, request_data, response_data);
		obs_source_release(source);
	} else {
		DARRAY(obs_source_t *) sources = {0};
		obs_enum_sources(find_source, &sources);
		obs_enum_scenes(find_source, &sources);
		if (!sources.num) {
			obs_data_set_string(response_data, "error", "no source found");
			obs_data_set_bool(response_data, "success", false);
			return;
		}
		for (size_t i = 0; i < sources.num; i++) {
			success = split_record_source(sources.array[i], request_data, response_data) && success;
		}
		da_free(sources);
	}
	obs_data_set_bool(response_data, "success", success);
}

static void websocket_add_chapter_record(obs_data_t *request_data, obs_data_t *response_data, void *param)
{
	UNUSED_PARAMETER(param);
	const char *source_name = obs_data_get_string(request_data, "source");
	bool success = true;
	if (strlen(source_name)) {
		obs_source_t *source = obs_get_source_by_name(source_name);
		if (!source) {
			obs_data_set_string(response_data, "error", "source not found");
			obs_data_set_bool(response_data, "success", false);
			return;
		}
		success = add_chapter_record_source(source, request_data, response_data);
		obs_source_release(source);
	} else {
		DARRAY(obs_source_t *) sources = {0};
		obs_enum_sources(find_source, &sources);
		obs_enum_scenes(find_source, &sources);
		if (!sources.num) {
			obs_data_set_string(response_data, "error", "no source found");
			obs_data_set_bool(response_data, "success", false);
			return;
		}
		for (size_t i = 0; i < sources.num; i++) {
			success = add_chapter_record_source(sources.array[i], request_data, response_data) && success;
		}
		da_free(sources);
	}
	obs_data_set_bool(response_data, "success", success);
}

static void websocket_stop_record(obs_data_t *request_data, obs_data_t *response_data, void *param)
{
	UNUSED_PARAMETER(param);
	const char *source_name = obs_data_get_string(request_data, "source");
	bool success = true;
	if (strlen(source_name)) {
		obs_source_t *source = obs_get_source_by_name(source_name);
		if (!source) {
			obs_data_set_string(response_data, "error", "source not found");
			obs_data_set_bool(response_data, "success", false);
			return;
		}
		success = stop_record_source(source, request_data, response_data);
		obs_source_release(source);
	} else {
		DARRAY(obs_source_t *) sources = {0};
		obs_enum_sources(find_source, &sources);
		obs_enum_scenes(find_source, &sources);
		if (!sources.num) {
			obs_data_set_string(response_data, "error", "no source found");
			obs_data_set_bool(response_data, "success", false);
			return;
		}
		for (size_t i = 0; i < sources.num; i++) {
			success = stop_record_source(sources.array[i], request_data, response_data) && success;
		}
		da_free(sources);
	}
	obs_data_set_bool(response_data, "success", success);
}

static bool start_replay_buffer_source(obs_source_t *source, obs_data_t *request_data, obs_data_t *response_data)
{
	obs_source_t *filter = get_source_record_filter(source, request_data, response_data, true);
	if (!filter)
		return false;
	obs_data_t *settings = obs_source_get_settings(filter);
	const char *filename = obs_data_get_string(request_data, "filename");
	struct source_record_filter_context *context = obs_obj_get_data(filter);
	if (context && context->output_active) {
		if (strlen(filename)) {
			if (strstr(filename, "%") || strcmp(filename, obs_data_get_string(settings, "filename_formatting")) != 0) {
				context->restart = true;
			}
		} else if (strstr(obs_data_get_string(settings, "filename_formatting"), "%")) {
			context->restart = true;
		}
	}

	if (strlen(filename))
		obs_data_set_string(settings, "filename_formatting", filename);

	obs_data_set_bool(settings, "replay_buffer", true);

	obs_source_update(filter, settings);
	obs_data_release(settings);

	obs_source_release(filter);
	return true;
}

static bool stop_replay_buffer_source(obs_source_t *source, obs_data_t *request_data, obs_data_t *response_data)
{
	obs_source_t *filter = get_source_record_filter(source, request_data, response_data, false);
	if (!filter)
		return false;

	obs_data_t *settings = obs_data_create();
	obs_data_set_bool(settings, "replay_buffer", false);
	obs_source_update(filter, settings);
	obs_data_release(settings);
	obs_source_release(filter);
	return true;
}

static bool save_replay_buffer_source(obs_source_t *source, obs_data_t *request_data, obs_data_t *response_data)
{
	obs_source_t *filter = get_source_record_filter(source, request_data, response_data, false);
	if (!filter)
		return false;
	struct source_record_filter_context *context = obs_obj_get_data(filter);
	if (!context->replayOutput) {
		blog(LOG_WARNING, "[Source Record] replay_buffer_save requested but replay output is not active");
		obs_source_release(filter);
		return false;
	}

	proc_handler_t *ph = obs_output_get_proc_handler(context->replayOutput);
	calldata_t cd = {0};
	bool success = proc_handler_call(ph, "save", &cd);
	calldata_free(&cd);
	if (!success)
		blog(LOG_WARNING, "[Source Record] replay_buffer_save proc call failed");
	else
		blog(LOG_INFO, "[Source Record] replay_buffer_save requested successfully");
	obs_source_release(filter);
	return success;
}

static void websocket_start_replay_buffer(obs_data_t *request_data, obs_data_t *response_data, void *param)
{
	UNUSED_PARAMETER(param);
	const char *source_name = obs_data_get_string(request_data, "source");
	bool success = true;
	if (strlen(source_name)) {
		obs_source_t *source = obs_get_source_by_name(source_name);
		if (!source) {
			obs_data_set_string(response_data, "error", "source not found");
			obs_data_set_bool(response_data, "success", false);
			return;
		}
		if (obs_data_get_bool(request_data, "stop_existing"))
			stop_replay_buffer_source(source, request_data, NULL);
		success = start_replay_buffer_source(source, request_data, response_data);
		obs_source_release(source);
	} else {
		DARRAY(obs_source_t *) sources = {0};
		obs_enum_sources(find_source, &sources);
		obs_enum_scenes(find_source, &sources);
		if (!sources.num) {
			obs_data_set_string(response_data, "error", "no source found");
			obs_data_set_bool(response_data, "success", false);
			return;
		}
		for (size_t i = 0; i < sources.num; i++) {
			success = start_replay_buffer_source(sources.array[i], request_data, response_data) && success;
		}
		da_free(sources);
	}
	obs_data_set_bool(response_data, "success", success);
}

static void websocket_stop_replay_buffer(obs_data_t *request_data, obs_data_t *response_data, void *param)
{
	UNUSED_PARAMETER(param);
	const char *source_name = obs_data_get_string(request_data, "source");
	bool success = true;
	if (strlen(source_name)) {
		obs_source_t *source = obs_get_source_by_name(source_name);
		if (!source) {
			obs_data_set_string(response_data, "error", "source not found");
			obs_data_set_bool(response_data, "success", false);
			return;
		}
		success = stop_replay_buffer_source(source, request_data, response_data);
		obs_source_release(source);
	} else {
		DARRAY(obs_source_t *) sources = {0};
		obs_enum_sources(find_source, &sources);
		obs_enum_scenes(find_source, &sources);
		if (!sources.num) {
			obs_data_set_string(response_data, "error", "no source found");
			obs_data_set_bool(response_data, "success", false);
			return;
		}
		for (size_t i = 0; i < sources.num; i++) {
			success = stop_replay_buffer_source(sources.array[i], request_data, response_data) && success;
		}
		da_free(sources);
	}
	obs_data_set_bool(response_data, "success", success);
}

static void websocket_save_replay_buffer(obs_data_t *request_data, obs_data_t *response_data, void *param)
{
	UNUSED_PARAMETER(param);
	const char *source_name = obs_data_get_string(request_data, "source");
	bool success = true;
	if (strlen(source_name)) {
		obs_source_t *source = obs_get_source_by_name(source_name);
		if (!source) {
			obs_data_set_string(response_data, "error", "source not found");
			obs_data_set_bool(response_data, "success", false);
			return;
		}
		success = save_replay_buffer_source(source, request_data, response_data);
		obs_source_release(source);
	} else {
		DARRAY(obs_source_t *) sources = {0};
		obs_enum_sources(find_source, &sources);
		obs_enum_scenes(find_source, &sources);
		if (!sources.num) {
			obs_data_set_string(response_data, "error", "no source found");
			obs_data_set_bool(response_data, "success", false);
			return;
		}
		for (size_t i = 0; i < sources.num; i++) {
			success = save_replay_buffer_source(sources.array[i], request_data, response_data) && success;
		}
		da_free(sources);
	}
	if (!success)
		blog(LOG_WARNING, "[Source Record] websocket replay_buffer_save finished with failures");
	else
		blog(LOG_INFO, "[Source Record] websocket replay_buffer_save finished successfully");
	obs_data_set_bool(response_data, "success", success);
}

static bool start_stream_source(obs_source_t *source, obs_data_t *request_data, obs_data_t *response_data)
{
	obs_source_t *filter = get_source_record_filter(source, request_data, response_data, true);
	if (!filter)
		return false;
	obs_data_t *settings = obs_source_get_settings(filter);

	const char *server = obs_data_get_string(request_data, "server");
	if (server && strlen(server))
		obs_data_set_string(settings, "server", server);

	const char *key = obs_data_get_string(request_data, "key");
	if (key && strlen(key))
		obs_data_set_string(settings, "key", key);

	obs_data_set_int(settings, "stream_mode", OUTPUT_MODE_ALWAYS);

	obs_source_update(filter, settings);
	obs_data_release(settings);

	obs_source_release(filter);
	return true;
}

static bool stop_stream_source(obs_source_t *source, obs_data_t *request_data, obs_data_t *response_data)
{
	obs_source_t *filter = get_source_record_filter(source, request_data, response_data, false);
	if (!filter)
		return false;

	obs_data_t *settings = obs_data_create();
	obs_data_set_int(settings, "stream_mode", OUTPUT_MODE_NONE);
	obs_source_update(filter, settings);
	obs_data_release(settings);
	obs_source_release(filter);
	return true;
}

static void websocket_start_stream(obs_data_t *request_data, obs_data_t *response_data, void *param)
{
	UNUSED_PARAMETER(param);
	const char *source_name = obs_data_get_string(request_data, "source");
	bool success = true;
	if (strlen(source_name)) {
		obs_source_t *source = obs_get_source_by_name(source_name);
		if (!source) {
			obs_data_set_string(response_data, "error", "source not found");
			obs_data_set_bool(response_data, "success", false);
			return;
		}
		if (obs_data_get_bool(request_data, "stop_existing"))
			stop_stream_source(source, request_data, NULL);
		success = start_stream_source(source, request_data, response_data);
		obs_source_release(source);
	} else {
		DARRAY(obs_source_t *) sources = {0};
		obs_enum_sources(find_source, &sources);
		obs_enum_scenes(find_source, &sources);
		if (!sources.num) {
			obs_data_set_string(response_data, "error", "no source found");
			obs_data_set_bool(response_data, "success", false);
			return;
		}
		for (size_t i = 0; i < sources.num; i++) {
			success = start_stream_source(sources.array[i], request_data, response_data) && success;
		}
		da_free(sources);
	}
	obs_data_set_bool(response_data, "success", success);
}

static void websocket_stop_stream(obs_data_t *request_data, obs_data_t *response_data, void *param)
{
	UNUSED_PARAMETER(param);
	const char *source_name = obs_data_get_string(request_data, "source");
	bool success = true;
	if (strlen(source_name)) {
		obs_source_t *source = obs_get_source_by_name(source_name);
		if (!source) {
			obs_data_set_string(response_data, "error", "source not found");
			obs_data_set_bool(response_data, "success", false);
			return;
		}
		success = stop_stream_source(source, request_data, response_data);
		obs_source_release(source);
	} else {
		DARRAY(obs_source_t *) sources = {0};
		obs_enum_sources(find_source, &sources);
		obs_enum_scenes(find_source, &sources);
		if (!sources.num) {
			obs_data_set_string(response_data, "error", "no source found");
			obs_data_set_bool(response_data, "success", false);
			return;
		}
		for (size_t i = 0; i < sources.num; i++) {
			success = stop_stream_source(sources.array[i], request_data, response_data) && success;
		}
		da_free(sources);
	}
	obs_data_set_bool(response_data, "success", success);
}

bool obs_module_load(void)
{
	blog(LOG_INFO, "[Source Record] loaded version %s", PROJECT_VERSION);
	obs_register_source(&source_record_filter_info);

	da_init(source_record_filters);

	vendor = obs_websocket_register_vendor("source-record");
	obs_websocket_vendor_register_request(vendor, "record_start", websocket_start_record, NULL);
	obs_websocket_vendor_register_request(vendor, "record_pause", websocket_pause_record, NULL);
	obs_websocket_vendor_register_request(vendor, "record_unpause", websocket_unpause_record, NULL);
	obs_websocket_vendor_register_request(vendor, "record_split", websocket_split_record, NULL);
	obs_websocket_vendor_register_request(vendor, "record_add_chapter", websocket_add_chapter_record, NULL);
	obs_websocket_vendor_register_request(vendor, "record_stop", websocket_stop_record, NULL);
	obs_websocket_vendor_register_request(vendor, "replay_buffer_start", websocket_start_replay_buffer, NULL);
	obs_websocket_vendor_register_request(vendor, "replay_buffer_stop", websocket_stop_replay_buffer, NULL);
	obs_websocket_vendor_register_request(vendor, "replay_buffer_save", websocket_save_replay_buffer, NULL);
	obs_websocket_vendor_register_request(vendor, "stream_start", websocket_start_stream, NULL);
	obs_websocket_vendor_register_request(vendor, "stream_stop", websocket_stop_stream, NULL);

	return true;
}

void obs_module_post_load(void)
{
#ifdef _WIN32
	void *handle = os_dlopen("obs");
#else
	void *handle = dlopen(NULL, RTLD_LAZY);
#endif
	if (handle) {
		obs_encoder_set_frame_rate_divisor_func =
			(bool (*)(obs_encoder_t *, uint32_t))os_dlsym(handle, "obs_encoder_set_frame_rate_divisor");
		obs_encoder_set_gpu_scale_type_func =
			(void (*)(obs_encoder_t *, enum obs_scale_type))os_dlsym(handle, "obs_encoder_set_gpu_scale_type");
		os_dlclose(handle);
	}
}

void obs_module_unload(void)
{
	da_free(source_record_filters);
}

const char *obs_module_name(void)
{
	return obs_module_text("SourceRecord");
}
