"""Reference answers; keep separate from the learner exercises."""

from collections import Counter


def longest_unique_substring(text: str) -> int:
    last_seen: dict[str, int] = {}
    left = best = 0
    for right, character in enumerate(text):
        if character in last_seen:
            left = max(left, last_seen[character] + 1)
        last_seen[character] = right
        best = max(best, right - left + 1)
    return best


def _ascii_alnum(character: str) -> bool:
    return "a" <= character <= "z" or "A" <= character <= "Z" or "0" <= character <= "9"


def is_palindrome(text: str) -> bool:
    left, right = 0, len(text) - 1
    while left < right:
        if not _ascii_alnum(text[left]):
            left += 1
        elif not _ascii_alnum(text[right]):
            right -= 1
        elif text[left].lower() != text[right].lower():
            return False
        else:
            left += 1
            right -= 1
    return True


def group_anagrams(words: list[str]) -> list[list[str]]:
    groups: dict[tuple[int, ...], list[str]] = {}
    for word in words:
        counts = [0] * 26
        for character in word:
            if not "a" <= character <= "z":
                raise ValueError("Words must contain only lowercase Latin letters")
            counts[ord(character) - ord("a")] += 1
        groups.setdefault(tuple(counts), []).append(word)
    return list(groups.values())


def compress(chars: list[str]) -> int:
    read = write = 0
    while read < len(chars):
        character = chars[read]
        end = read + 1
        while end < len(chars) and chars[end] == character:
            end += 1
        chars[write] = character
        write += 1
        count = end - read
        if count > 1:
            divisor = 1
            while divisor <= count // 10:
                divisor *= 10
            while divisor:
                chars[write] = chr(ord("0") + count // divisor)
                write += 1
                count %= divisor
                divisor //= 10
        read = end
    return write


def minimum_window(text: str, target: str) -> str:
    if not target or len(target) > len(text):
        return ""
    needed = Counter(target)
    missing = len(target)
    left = best_start = 0
    best_length = len(text) + 1
    for right, character in enumerate(text):
        if needed[character] > 0:
            missing -= 1
        needed[character] -= 1
        while missing == 0:
            length = right - left + 1
            if length < best_length:
                best_start, best_length = left, length
            outgoing = text[left]
            needed[outgoing] += 1
            if needed[outgoing] > 0:
                missing += 1
            left += 1
    if best_length > len(text):
        return ""
    return text[best_start : best_start + best_length]
