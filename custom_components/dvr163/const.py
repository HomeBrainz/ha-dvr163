"""Constants for the DVR163 IP Camera integration."""

DOMAIN = "dvr163"

DEFAULT_PORT = 80
DEFAULT_USERNAME = "admin"

# This firmware family always exposes both a main (full resolution) and a
# sub (lower resolution, e.g. 640x720 vs 1920x2160) stream at these two
# fixed paths -- confirmed identical content/audio, just different
# resolution. Every camera gets both as separate entities rather than
# forcing a choice at setup time.
STREAM_PATH_MAIN = "/livestream/11"
STREAM_PATH_SUB = "/livestream/12"
STREAMS = {
    "main": (STREAM_PATH_MAIN, "Main"),
    "sub": (STREAM_PATH_SUB, "Sub"),
}

# RTP payload types this firmware family uses for its two tracks, per the
# session SDP announced in the /livestream/N preamble. Not standard/dynamic
# negotiation -- confirmed fixed on every unit tested so far.
PT_VIDEO_H265 = 97
PT_AUDIO_AAC = 104

PRESET_COUNT = 4

# Image attributes are read/written together as a group -- this firmware's
# setimageattr silently resets any omitted attribute to 127. See README.
IMAGE_ATTR_KEYS = ("hue", "brightness", "saturation", "contrast")
