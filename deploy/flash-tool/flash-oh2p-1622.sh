#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
UPDATE_BIN="$SCRIPT_DIR/macos/update"
FIRMWARE="$SCRIPT_DIR/../client-patch/assets/mico_all_616cd9d93_1.62.2/root-patched.squashfs"
EXPECTED_SHA256="ec50636a2485a62bc21d1f89f18544faf093cd770df76b742b4d78011ae16132"
SYSTEM0_MAX_SIZE=$((0x02800000))

die() {
    printf 'ERROR: %s\n' "$*" >&2
    exit 1
}

has_update_error() {
    grep -Eiq 'ERR|Fail|error_msg' <<<"$1"
}

run_update_checked() {
    local output
    local status

    printf '\nRunning:'
    printf ' %q' "$UPDATE_BIN" "$@"
    printf '\n'

    if output=$("$UPDATE_BIN" "$@" 2>&1); then
        status=0
    else
        status=$?
    fi
    printf '%s\n' "$output"

    if (( status != 0 )) || has_update_error "$output"; then
        die "Amlogic command failed; no later flashing step was run."
    fi
}

[[ $(uname -s) == Darwin ]] || die "This script only supports macOS."
[[ -x "$UPDATE_BIN" ]] || die "Missing executable: $UPDATE_BIN"
[[ -f "$FIRMWARE" ]] || die "Missing firmware: $FIRMWARE"

firmware_type=$(file "$FIRMWARE")
grep -q 'Squashfs filesystem' <<<"$firmware_type" || die "Firmware is not SquashFS: $firmware_type"

firmware_size=$(stat -f %z "$FIRMWARE")
(( firmware_size < SYSTEM0_MAX_SIZE )) || die "Firmware is too large for system0: $firmware_size bytes"

firmware_sha256=$(shasum -a 256 "$FIRMWARE" | awk '{print $1}')
[[ "$firmware_sha256" == "$EXPECTED_SHA256" ]] || die "Firmware SHA-256 mismatch: $firmware_sha256"

printf 'Validated firmware:\n'
printf '  Path: %s\n' "$FIRMWARE"
printf '  Size: %s bytes\n' "$firmware_size"
printf '  SHA-256: %s\n' "$firmware_sha256"
printf '  Target: OH2P system0\n'
printf '  Final boot slot: boot0\n\n'

read -r -p 'Type FLASH to continue: ' confirmation
[[ "$confirmation" == FLASH ]] || die "Cancelled."

printf '\nKeep the USB data cable connected. Unplug speaker power and wait until its light is fully off.\n'
read -r -p 'Press Enter when power is unplugged and the light is off. '
printf 'Plug speaker power in NOW. Waiting for the WorldCup USB window'

attempt=0
while true; do
    if identify_output=$("$UPDATE_BIN" identify 7 2>&1); then
        identify_status=0
    else
        identify_status=$?
    fi

    if (( identify_status == 0 )) && ! has_update_error "$identify_output" \
        && grep -q 'This firmware version is' <<<"$identify_output"; then
        printf '\n%s\n' "$identify_output"
        printf 'WorldCup device connected. Do not unplug power or USB.\n'
        break
    fi

    attempt=$((attempt + 1))
    if (( attempt % 10 == 0 )); then
        printf '.'
    fi
    sleep 0.05
done

# Persist a recovery-friendly boot delay while boot1 is still selected.
run_update_checked bulkcmd "     setenv bootdelay 15"
run_update_checked bulkcmd "     saveenv"

# Write system0 before selecting boot0. A failed write therefore leaves the
# currently working factory boot1 selected.
run_update_checked partition system0 "$FIRMWARE"

# Select the newly written patched system only after the partition write passed.
run_update_checked bulkcmd "     setenv boot_part boot0"
run_update_checked bulkcmd "     saveenv"
run_update_checked bulkcmd "     printenv boot_part"

printf '\nFLASH COMPLETE.\n'
printf 'Unplug the USB cable and speaker power, wait 10 seconds, then reconnect speaker power.\n'
printf 'The factory 1.62.2 system remains in boot1; patched 1.62.2 is now in boot0.\n'
