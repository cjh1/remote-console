#!/bin/bash
# A script to broadcast a message to all logged-in users' TTYs

broadcast_message() {
    local msg="$1"

    # Check if message provided
    if [ -z "$msg" ]; then
        echo "Usage: $0 'your message'"
        exit 1
    fi

    # Get all TTYs from ps
    for tty in $(ps -eo tty | grep -E 'pts' | sort -u); do
        # Write to the device
        echo -e "$msg" > "/dev/$tty" 2>/dev/null
    done
}

# Call with first argument
broadcast_message "$1"
