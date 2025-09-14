# filters.py
from transformers import pipeline, DistilBertTokenizer, DistilBertForSequenceClassification
import torch
import torch.nn.functional as F

KEYWORDS = [
    "replace","remove","oust","vote out","impeach","recall","fire",
    "elect","appoint","nominate","re-elect","install","sack",
    "amend","update charter","bylaw","constitution","modify rules",
    "change rules","adjust policy","alter policy","policy change",
    "revise terms","ratify","motion to change",
    "withdraw funds","transfer funds","fund allocation","reallocate budget",
    "budget change","treasury control","redirect funds","cut funding","fund freeze",
    "investigate","audit","review","accountability","watchdog","reporting",
    "oversight","remove steward","replace facilitator",
    "quorum","voting threshold","change voting power","delegate removal",
    "remove delegation","change voting rights","adjust voting weight",
    "pivot","terminate project","halt","pause","sunset","wind down",
    "stop funding","cancel","scrap","shut down","cease operations",
    "lawsuit","litigation","legal","ban","blacklist","suspend","penalize",
    "take control","seize","reclaim","redeploy","change leadership",
    "power shift","redistribute power"
]
KEYWORDS = [k.lower() for k in KEYWORDS]


def build_zero_shot_classifier(model_name="facebook/bart-large-mnli"):
    print("[filters] Loading zero-shot classifier...")
    return pipeline("zero-shot-classification", model=model_name)


def filter_zero_shot(proposals, classifier, threshold=0.6):
    print(f"[filters] Running zero-shot on {len(proposals)} proposals...")
    accepted = []
    labels = ["activist", "non-activist"]
    hypothesis_template = "This proposal is {} because it involves governance activism, strategic control changes, treasury reallocation, or leadership changes."

    for p in proposals:
        body = (p.get("description") or "").lower()
        if not any(k in body for k in KEYWORDS):
            continue
        try:
            res = classifier(body, candidate_labels=labels, hypothesis_template=hypothesis_template)
            top_label = res["labels"][0]
            top_score = float(res["scores"][0])
            if top_label == "activist" and top_score >= threshold:
                p["_zero_shot_score"] = top_score
                accepted.append(p)
        except Exception as e:
            print(f"[filters] classifier error: {e}")
            continue
    print(f"[filters] {len(accepted)} proposals accepted by zero-shot")
    return accepted


def build_distilbert_classifier(model_name="distilbert-base-uncased"):
    print("[filters] Loading DistilBERT model...")
    tokenizer = DistilBertTokenizer.from_pretrained(model_name)
    model = DistilBertForSequenceClassification.from_pretrained(model_name, num_labels=2)
    model.eval()
    return tokenizer, model


def filter_distilbert(proposals, tokenizer, model, device="cpu"):
    print(f"[filters] Running DistilBERT on {len(proposals)} proposals...")
    final = []
    if device != "cpu":
        model.to(device)

    for p in proposals:
        body = p.get("description") or ""
        if not body.strip():
            continue
        try:
            enc = tokenizer(body, truncation=True, padding=True, max_length=256, return_tensors="pt")
            if device != "cpu":
                enc = {k: v.to(device) for k, v in enc.items()}
            with torch.no_grad():
                outputs = model(**enc)
                logits = outputs.logits
                probs = F.softmax(logits, dim=-1).cpu().numpy()[0]  # [non-activist, activist]
                prob_activist = float(probs[1])
                predicted = int(probs.argmax())
                p["_distilbert_prob_activist"] = prob_activist
                p["_distilbert_pred"] = predicted
                if predicted == 1 or prob_activist >= 0.5:
                    final.append(p)
        except Exception as e:
            print(f"[filters] DistilBERT error on proposal id {p.get('id')}: {e}")
            continue

    print(f"[filters] {len(final)} proposals accepted by DistilBERT")
    return final
