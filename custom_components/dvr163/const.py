"""Constants for the DVR163 IP Camera integration."""

DOMAIN = "dvr163"

CONF_STREAM_PATH = "stream_path"

DEFAULT_PORT = 80
DEFAULT_USERNAME = "admin"
DEFAULT_STREAM_PATH = "/livestream/11"

# RTP payload types this firmware family uses for its two tracks, per the
# session SDP announced in the /livestream/N preamble. Not standard/dynamic
# negotiation -- confirmed fixed on every unit tested so far.
PT_VIDEO_H265 = 97
PT_AUDIO_AAC = 104

PRESET_COUNT = 4

# Image attributes are read/written together as a group -- this firmware's
# setimageattr silently resets any omitted attribute to 127. See README.
IMAGE_ATTR_KEYS = ("hue", "brightness", "saturation", "contrast")
