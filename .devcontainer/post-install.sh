#!/bin/bash
set -ex

##
## Create some aliases
##
echo 'alias ll="ls -alF"' >> $HOME/.bashrc
echo 'alias la="ls -A"' >> $HOME/.bashrc
echo 'alias l="ls -CF"' >> $HOME/.bashrc

# Convenience workspace directory for later use
WORKSPACE_DIR=$(pwd)

# Keep the venv in the workspace so the interpreter path in devcontainer.json resolves
poetry config virtualenvs.in-project true
# Now install all dependencies
poetry install

echo "Done!"
