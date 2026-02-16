from looplane.task.register import TaskRegistry


def make_function(name, module):
    """Create a top-level like function dynamically."""
    def temp():
        pass

    # simulate a real top-level function
    temp.__name__ = name
    temp.__qualname__ = name
    temp.__module__ = module
    return temp


def test_same_function_name_different_modules():
    """
    Two tasks with the same function name but different modules
    should both exist in the registry and resolve correctly.
    """
    registry = TaskRegistry()

    func1 = make_function("send", "module_a")
    func2 = make_function("send", "module_b")

    registry.register(func1)
    registry.register(func2)

    fqn1 = "module_a.send"
    fqn2 = "module_b.send"

    assert registry.get(fqn1) is func1
    assert registry.get(fqn2) is func2


def test_legacy_short_name_still_resolves():
    """
    Backward compatibility:
    Old serialized tasks stored only the short name.
    They should still resolve after the fix.
    """
    registry = TaskRegistry()

    func = make_function("process", "module_x")

    registry.register(func)

    # old behavior lookup
    assert registry.get("process") is func


def test_short_name_alias_is_deterministic():
    """
    If two modules define the same function name,
    the short-name alias should consistently resolve
    to the first registered function.
    """
    registry = TaskRegistry()

    func1 = make_function("send", "module_a")
    func2 = make_function("send", "module_b")

    registry.register(func1)
    registry.register(func2)

    # short name should not randomly switch
    assert registry.get("send") is func1

