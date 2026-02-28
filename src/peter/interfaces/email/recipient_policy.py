from __future__ import annotations

from typing import Iterable, Tuple


def _norm(addr: str) -> str:
    return (addr or "").strip().lower()


def is_internal(addr: str, internal_domain: str) -> bool:
    a = _norm(addr)
    return a.endswith("@" + internal_domain.lower())


def dedupe(seq: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for x in seq:
        x = _norm(x)
        if not x or x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def build_sanitized_reply_recipients(
    *,
    internal_domain: str,
    original_from: str,
    original_to: list[str],
    original_cc: list[str],
    bot_mailbox: str,
    forced_cc: list[str],
    max_recipients: int = 10,
) -> Tuple[list[str], list[str]]:
    """Build internal-only recipients for in-thread reply.

    Updated safety policy (2026-02):
    - NEVER email external domains.
    - NEVER "reply-all" to the original internal To/Cc list (prevents wide internal blasts).
    - To: internal sender only (if internal).
    - Cc: forced internal list only.
    - Hard cap total recipients as a fuse.
    """

    bot = _norm(bot_mailbox)

    to_candidates: list[str] = []
    cc_candidates: list[str] = []

    if is_internal(original_from, internal_domain) and _norm(original_from) != bot:
        to_candidates.append(original_from)

    for a in forced_cc or []:
        a = _norm(a)
        if a and a != bot and is_internal(a, internal_domain):
            cc_candidates.append(a)

    to_list = dedupe(to_candidates)
    cc_list = [a for a in dedupe(cc_candidates) if a not in set(to_list)]

    if not to_list:
        # If sender is external, send only to forced internal list.
        to_list = cc_list[:]
        cc_list = []

    # Fuse: cap total recipients
    total = len(to_list) + len(cc_list)
    if total > max_recipients:
        # Trim CC first; keep To intact.
        room = max_recipients - len(to_list)
        cc_list = cc_list[: max(0, room)]

    return to_list, cc_list


def assert_internal_only(to_list: list[str], cc_list: list[str], *, internal_domain: str) -> None:
    for a in list(to_list) + list(cc_list):
        if not is_internal(a, internal_domain):
            raise RuntimeError(f"External recipient detected: {a}")
