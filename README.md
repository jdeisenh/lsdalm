LSDALM

Work in Progress

Live Stream Dumper-Analyzer-Looper-Monitor

This is right now, just a scribble of rough design ideas

-- record 
store all segments to disk
- with/without media segments
- whole dvr or cutoff
- all or some tracks
- allow for several independent recordings

-- check
- verify the stream, optionally with segment timestamp and format
- no store
- logging and alarming
- parameters checked
-- manifest http errors
-- time to respond
-- parsability
-- live edge distance
-- jitter in live edge
-- dvr window length
-- number of live periods >1
-- period continuity
-- segment accessability 
-- segment timestamps matching

-- fetch
- Allow access by fixed date 
_ Get last manifest at this date, 404 before and after
- rewrite basURL/representations according to availability to make it valid

-- loop
Play a loop of the recording
- provide a fixed time offset (or open a session)
- start (simplification) with the manifest of that date, as in fetch
- assume for simplicity it in the recording range
- send this manifest with the presentationTimeOffset adjusted to move the live edge to the current time (best would be same live delay as recording)
- loop points are before and after the recording
- function to append shifted&cut periods
- cutting need not be implemented right now, pto shift will do
x function to extract first and last availabilty from manifest 

How to map
Original location should be mapped as-is
Cut at multiple of segment size (for encoders that keep the order)
cut outside of ad break

Request has absolute time, redirects to offset
shift is multiples of duration
on the seam:
old manifest as is (assume it has a clear cutoff)
new manifest if from the beginning, with beging cut of to the splice
maybe always concatenate before, now and after, and run a prune on it

