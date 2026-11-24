import re

# Common credential formats. Memory is shown to the model on every run, so secrets stay out.
_SECRET = re.compile(
    r"sk-ant-[A-Za-z0-9_-]{8,}|sk-[A-Za-z0-9]{20,}|gh[pousr]_[A-Za-z0-9]{20,}|AKIA[0-9A-Z]{16}"
    r"|xox[abpr]-[A-Za-z0-9-]{10,}|-----BEGIN [A-Z ]*PRIVATE KEY-----"
)


def check_text(label: str, text: str | None, limit: int) -> None:
    """Refuse text that is too long or looks like it contains a credential."""
    if text is None:
        return
    if len(text) > limit:
        raise ValueError(f"{label} is {len(text)} characters; the limit is {limit}.")
    if _SECRET.search(text):
        raise ValueError(f"{label} looks like it contains a secret. Don't store credentials.")
