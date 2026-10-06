from identity import ActorKind, classify_actor

BOT_UID = 111
OWNER_UID = 222


def test_classify_self():
    actor = classify_actor(BOT_UID, "ThycaBot", bot_uid=BOT_UID, owner_uid=OWNER_UID)
    assert actor.kind is ActorKind.SELF
    assert actor.uid == BOT_UID
    assert actor.display_name == "ThycaBot"


def test_classify_owner():
    actor = classify_actor(OWNER_UID, "Hoa", bot_uid=BOT_UID, owner_uid=OWNER_UID)
    assert actor.kind is ActorKind.OWNER


def test_classify_other():
    actor = classify_actor(999, "SomeUser", bot_uid=BOT_UID, owner_uid=OWNER_UID)
    assert actor.kind is ActorKind.OTHER


def test_self_wins_on_misconfiguration():
    # If config ever sets owner == bot, treat as self (never grant owner powers blindly).
    actor = classify_actor(BOT_UID, "X", bot_uid=BOT_UID, owner_uid=BOT_UID)
    assert actor.kind is ActorKind.SELF
