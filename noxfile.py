import os
import re
import sys
from functools import lru_cache
from typing import Any, Dict

import nox
from nox.command import CommandFailed
from nox.sessions import Session
from rtoml import load

os.environ.update({"PDM_IGNORE_SAVED_PYTHON": "1"})

"""
# Quick start for Nox
 
## Command Usage:

- `nox` - run all sessions
- `nox -l | --list | --list-sessions` - show sessions
- `nox -s | -e | --session <session_name>` - run a session
- `nox -p | --python 3.7 3.8` - run sessions with given Python versions
- You can also use pytest-style keywords using `-k` or `--keywords`,
  and tags using `-t` or `--tags` to filter test sessions:
  - `nox -k "not lint"`
  - `nox -k "tests and not lint"`
  - `nox -k "not my_tag"`
  - `nox -t "my_tag" "my_other_tag"`
- `nox --stop-on-first-error` - stop running sessions if one fails
- [Passing arguments into sessions]
  (https://nox.thea.codes/en/stable/config.html#passing-arguments-into-sessions)
- [Parametrizing sessions]
  (https://nox.thea.codes/en/stable/config.html#parametrizing-sessions)

References:
- [API](https://nox.thea.codes/en/stable/config.html#configuration-api)
- [Command Line Usage](https://nox.thea.codes/en/stable/usage.html#command-line-usage)

## Shell Completion

- Bash:

  `eval "$(register-python-argcomplete nox)"`

- Zsh:

  ``` shell
  # To activate completions for zsh you need to have
  # bashcompinit enabled in zsh:
  autoload -U bashcompinit
  bashcompinit

  # Afterwards you can enable completion for Nox:
  eval "$(register-python-argcomplete nox)"
  ```

For more shells refer to:
[Shell Completion](https://nox.thea.codes/en/stable/usage.html#shell-completion)
"""


@lru_cache(maxsize=1)
def get_pyproject_toml() -> Dict[str, Any]:
    with open("pyproject.toml") as fp:
        return load(fp)


@lru_cache(maxsize=1)
def get_python_version() -> str:
    pyproject = get_pyproject_toml()
    if m := re.search(r">=\s*(\d+(\.\d+)*)", pyproject["project"]["requires-python"]):
        return m.group(1)
    else:
        return f"{sys.version_info.major}.{sys.version_info.minor}"


@lru_cache(maxsize=1)
def get_dev_dependencies() -> Dict[str, str]:
    pyproject = get_pyproject_toml()
    pat = re.compile(r"[ <>~=]")
    dev_deps: Dict[str, str] = {}
    for dep in pyproject["dependency-groups"]["dev"]:
        sep = -1
        for m in pat.finditer(dep):
            sep = m.span()[0]
            break
        if sep == -1:
            dev_deps[dep] = dep
        else:
            dev_deps[dep[:sep]] = dep
    return dev_deps


AUTOFLAKE_VERSION = get_dev_dependencies()["autoflake"]
MYPY_VERSION = get_dev_dependencies()["mypy"]
RUFF_VERSION = get_dev_dependencies()["ruff"]
SOURCES = ["fluentmap.py", "noxfile.py", "tests"]


@nox.session(python=False)
def shell_completion(session: Session):
    shell = os.getenv("SHELL")
    if shell is None or "bash" in shell:
        session.log('eval "$(register-python-argcomplete nox)"')
    elif "zsh" in shell:
        session.log("autoload -U bashcompinit")
        session.log("bashcompinit")
        session.log('eval "$(register-python-argcomplete nox)"')
    elif "tcsh" in shell:
        session.log("eval `register-python-argcomplete --shell tcsh nox`")
    elif "fish" in shell:
        session.log("register-python-argcomplete --shell fish nox | .")
    else:
        session.log('eval "$(register-python-argcomplete nox)"')


@nox.session(python=False)
def clean(session: Session):
    session.run(
        "rm",
        "-rf",
        ".mypy_cache",
        ".pytype",
        ".pytest_cache",
        ".pytype_output",
        "build",
        "dist",
        "html_cov",
        "html_doc",
        "logs",
    )
    session.run(
        "sh",
        "-c",
        "find . | grep -E '(__pycache__|\.pyc|\.pyo$$)' | xargs rm -rf",
    )


@nox.session(python="3.8", reuse_venv=True)
@nox.parametrize("autoflake", [AUTOFLAKE_VERSION])
@nox.parametrize("ruff", [RUFF_VERSION])
def format(session: Session, autoflake: str, ruff: str):
    session.install(autoflake, ruff)
    try:
        session.run("taplo", "fmt", "pyproject.toml", external=True)
    except CommandFailed:
        session.warn(
            "Seems that `taplo` is not found, skip formatting `pyproject.toml`. "
            "(Refer to https://taplo.tamasfe.dev/ for information on how to install "
            "`taplo`)"
        )
    session.run("autoflake", "--version")
    session.run("autoflake", *SOURCES)
    session.run("ruff", "--version")
    session.run("ruff", "format", *SOURCES)


@nox.session(python="3.8", reuse_venv=True)
@nox.parametrize("autoflake", [AUTOFLAKE_VERSION])
@nox.parametrize("ruff", [RUFF_VERSION])
def format_check(session: Session, autoflake: str, ruff: str):
    session.install(autoflake, ruff)
    try:
        session.run("taplo", "check", "pyproject.toml", external=True)
    except CommandFailed:
        session.warn(
            "Seems that `taplo` is not found, skip checking `pyproject.toml`. "
            "(Refer to https://taplo.tamasfe.dev/ for information on how to install "
            "`taplo`)"
        )
    session.run("autoflake", "--version")
    session.run("autoflake", "--check-diff", *SOURCES)
    session.run("ruff", "--version")
    session.run("ruff", "format", "--check", "--diff", *SOURCES)


@nox.session(python="3.8", reuse_venv=True)
@nox.parametrize("mypy", [MYPY_VERSION])
def mypy(session: Session, mypy: str):
    session.install(mypy)
    session.run("mypy", "--version")
    session.log(
        "If you encountered "
        "\"AttributeError: attribute 'TypeInfo' of '_fullname' undefined\", "
        "please try to execute `rm -rf .mypy_cache`"
    )
    session.run("mypy", "fluentmap.py", "noxfile.py")


@nox.session(python=False)
def test(session: Session):
    session.run("pytest", "tests")


@nox.session(reuse_venv=True)
def test_for_ci(session: Session):
    session.install(
        "coverage[toml]",
        "pytest",
        "pytest-asyncio",
        "pytest-cov",
        "pytest-mock",
        "pytest-timeout",
    )
    test(session)
