# config.py
BASE_URL = "https://tally.xyz/explore"
DAO_CARD_SELECTOR = ".dao-card"          # DAO links on explore page
PROPOSAL_CARD_SELECTOR = ".proposal-card" # proposals inside DAO
LOAD_MORE_BUTTON_SELECTOR = "button:has-text('Load More')"

# Keys that must exist in a proposal
REQUIRED_KEYS = [
    "dao",
    "id",
    "title",
    "description",
    "link",
    "state",
    "createdAt",
    "proposer",
]

PAGE_SIZE = 20  # Items per scroll/load
