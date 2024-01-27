#!/bin/sh

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"

if [ -z "$1" ]; then
    echo "Usage: create-hw.sh HW_NAME [--uses-dslib]"
    exit 1
fi

HW_NAME="$1"
if [ "$2" = "--uses-dslib" ]; then
    JOB_TEMPLATE=".hw-dslib-test"
elif [ "$2" = "" ]; then
    JOB_TEMPLATE=".hw-test"
else
    echo "Unknown option: $2"
    echo "Usage: create-hw.sh HW_NAME [--uses-dslib]"
    exit 1
fi

HW_DIR="$BASE_DIR/$HW_NAME"
mkdir -p "$HW_DIR"

JOB_FILE="$BASE_DIR/.gitlab-ci/$HW_NAME.yml"
printf "%s\n" \
"$HW_NAME:" \
"  extends: $JOB_TEMPLATE" \
"  image: ..." \
"  variables:" \
"    ..." \
"  services:" \
"    ..." \
"  before_script:" \
"    ..." \
"  script:" \
"    ..." \
> "$JOB_FILE"

echo "Created job template in $JOB_FILE"
echo "Dorectory for HW files is $HW_DIR"
