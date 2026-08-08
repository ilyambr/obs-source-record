# Source Record filter for OBS Studio

Plugin for OBS Studio to make sources available to record via a filter

# Download

https://obsproject.com/forum/resources/source-record.1285/

# Build
1. In-tree build
    - Build OBS Studio: https://obsproject.com/wiki/Install-Instructions
    - Check out this repository to plugins/source-record
    - Add `add_subdirectory(source-record)` to plugins/CMakeLists.txt
    - Rebuild OBS Studio

1. Stand-alone build (Linux only)
    - Verify that you have package with development files for OBS
    - Check out this repository and run `cmake -S . -B build -DBUILD_OUT_OF_TREE=On && cmake --build build`

# Donations
https://www.paypal.me/exeldro

# Changes

0.4.16 - unfroze OBS. yes it was actually freezing your whole program for 30 whole seconds if you disabled a filter mid-replay-buffer, we just never told you why. also taught the filter to snitch on itself (new get_record_status thing) so other plugins can ask "hey are you recording" instead of guessing. also the DLL was straight up lying about its own version number this whole time (kept saying 0.4.13 no matter what we shipped) - it has been informed it is in fact 0.4.16 now

0.4.14 - plugged a leak. an output that got torn down before it ever actually started (thanks, rapid duration-slider dragging and DPI-scaled window capture) never got freed, so it just sat there hoarding frames forever. if your replay buffer was mysteriously eating double-digit gigabytes, this was you, sorry

0.4.13 - the filter now yells "SAVED IT" (a replay_saved signal, with the file path attached, no extra legwork required) instead of you having to go ask. also gave it a save_replay_buffer button other plugins can press for it, named your audio tracks like a normal person (Track 1, Track 2...) instead of leaving them nameless, and taught it to auto-pause itself when you hide the source so it stops recording a black screen for you

0.4.12 - fixed the hotkey field lying to you. it was comparing the wrong pointer so it always said "no hotkey bound" even when you very much had one bound. it apologizes

0.4.11 - added a status API so other plugins can peek at whether your replay buffer is on/off/broken without prying the filter open. this is the whole reason obs-replay-slider exists now, you're welcome

0.4.10 - you can now smoosh other scene items (chat overlay, webcam, whatever) directly into the recording, in the right spot, at the right size, like they were never separate to begin with

0.4.9 - added more advanced multitrack audio
-ilyambr and claude fucking code:)
