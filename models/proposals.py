# models/proposal.py
from pydantic import BaseModel

class Proposal(BaseModel):
    dao: str
    id: str
    title: str
    description: str
    link: str
    state: str
    createdAt: str
    proposer: str
