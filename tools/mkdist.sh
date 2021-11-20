#!/usr/bin/env bash

# This program should be run from the databricks-cli directory.
#
# It assumes the directory is a 'clean' checkout, as it does not attempt
# to exclude extraneous files (aside from the contents of the .git/ subdir).

UC_VER="uc10"

# Check that databricks-cli is 'clean'
if ! git status | grep "nothing to commit, working tree clean"; then
    echo "PROBLEM: Current directory is not a clean checkout -- remove extraneous files"
    exit 1
fi

PY_VERSION="$(python -c 'from databricks_cli.version import version ; print(version)')"
echo "PY_VERSION = [$PY_VERSION]"
BASE_VERSION="$(echo $PY_VERSION | sed -e "s/\.dev0//")"
echo "BASE_VERSION = [$BASE_VERSION]"

WORK_DIR=${PWD}
echo "WORK_DIR=${WORK_DIR}"

echo "Creating distribution"
VERSION="${BASE_VERSION}.${UC_VER}"
echo "VERSION = [$VERSION]"

PKG_NAME="databricks-cli-${VERSION}"
TARFILE="${PKG_NAME}.tgz"

# Remove the .pyc files that were created when setting the PY_VERSION
rm databricks_cli/*.pyc

# Fix version.py with new version string
# Assume running on Mac (sed -i syntax is Mac-specific)
CMD="sed -i '' 's|${PY_VERSION}|${VERSION}|' databricks_cli/version.py"
eval $CMD

cd ..
# Add symlink (since Mac 'tar' doesn't support --transform)
ln -sf ${WORK_DIR} ${PKG_NAME}

tar -L -czf ${TARFILE} --exclude=.git --exclude=tools ${PKG_NAME}

# Remove symlink
rm  databricks-cli-${VERSION}
echo "Distribution created: ../$TARFILE"
cd $WORK_DIR
git reset --hard
echo "Done."
