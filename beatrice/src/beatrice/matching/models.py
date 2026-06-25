# The group-rerank relationship vocabulary. A single guidance proposition is
# judged against each surviving law candidate as exactly one of these.
RELATIONSHIP_TYPES = (
    "GROUNDED",             # guidance accurately reflects this law
    "UNGROUNDED",           # this law is not the right basis for the guidance
    "CONFLICTS",            # guidance contradicts this law (same framework)
    "GUIDANCE_INCOMPLETE",  # guidance covers only part of what this law requires
    "GUIDANCE_BROADER",     # guidance adds requirements beyond / above this law
    "GUIDANCE_MISSING",     # law states a requirement the guidance touches but skips
)
