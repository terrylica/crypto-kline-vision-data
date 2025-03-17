#!/bin/bash

# Disable "exit on error" for our interactive script
set +e

################################
# CONFIG AND HELPER FUNCTIONS
################################

DEBUG="${DEBUG:-0}"
[ "$DEBUG" = "1" ] && echo "DEBUG MODE ENABLED"

function debug_log() {
  [ "$DEBUG" = "1" ] && echo "DEBUG: $1" >&2
}

################################
# SSH KEY MANAGEMENT FUNCTION
################################

function verify_single_key() {
  # Function to verify only one key is active and fix if needed
  local num_keys active_key_info
  
  # Check current keys - use a better approach to count keys
  active_key_info=$(ssh-add -l 2>/dev/null)
  num_keys=$(echo "$active_key_info" | grep -E "SHA256|MD5" | wc -l)
  
  if [ "$num_keys" -eq 0 ]; then
    echo "⚠️ No SSH keys currently active"
    return 1
  elif [ "$num_keys" -gt 1 ]; then
    echo "⚠️ Multiple keys detected ($num_keys keys active)"
    echo "$active_key_info" | sed 's/^/   /'
    
    # Clear all keys and re-add just the selected key
    ssh-add -D > /dev/null
    
    if [ -n "$1" ]; then
      echo "🔑 Reactivating only key: $(basename "$1")"
      if ssh-add "$1" &>/dev/null || ssh-add "$1"; then
        # Double-check that we only have one key now
        [ "$(ssh-add -l 2>/dev/null | grep -E "SHA256|MD5" | wc -l)" -eq 1 ] && 
          echo "✅ Successfully activated single key" ||
          { echo "⚠️ Still detected multiple keys after cleanup attempt"; ssh-add -D > /dev/null; sleep 1; ssh-add "$1" &>/dev/null || ssh-add "$1"; }
        return 0
      else
        echo "❌ Failed to reactivate key"
        return 1
      fi
    else
      echo "❌ No key path provided for reactivation"
      return 1
    fi
  else
    echo "✅ Verified: Single key active"
    return 0
  fi
}

function manage_ssh_keys() {
  # This function handles scanning, displaying, and activating SSH keys
  # Returns: 0 = success, 1 = try again, 2 = fatal error
  
  clear
  echo -e "┌───────────────────────────────────────────┐\n│     🔐 GIT SSH AUTHENTICATION SETUP       │\n└───────────────────────────────────────────┘\n"
  echo "STEP 3: Managing SSH keys..."
  
  # Debug
  [ "$DEBUG" = "1" ] && set -x
  
  # Use the pre-scanned SSH keys from the global arrays
  if [ ${#ALL_SSH_KEYS[@]} -eq 0 ]; then
    echo "❌ No SSH keys found in ~/.ssh directory"
    echo "   You may need to set up SSH keys on your host machine"
    return 2  # Fatal error
  fi
  
  # Display available SSH keys
  echo "📋 Available SSH keys:"
  for i in "${!ALL_SSH_KEYS[@]}"; do
    key="${ALL_SSH_KEYS[$i]}"
    key_name=$(basename "$key")
    comment="${KEY_COMMENTS[$i]}"
    
    echo "   $((i+1))) $key_name${comment:+ ($comment)}"
  done
  
  # Show currently loaded keys
  echo -e "\n📋 Currently active SSH key:"
  
  # Capture ssh-add output and exit code
  local ssh_add_output ssh_add_exit_code current_key_fingerprint current_key_comment
  ssh_add_output=$(ssh-add -l 2>&1) || true
  ssh_add_exit_code=$?
  
  # ssh-add return codes: 0=keys found, 1=no keys, 2=no agent
  if [ $ssh_add_exit_code -eq 0 ] && [[ ! "$ssh_add_output" == *"no identities"* ]]; then
    # Keys found, show them
    echo "$ssh_add_output" | sed 's/^/   /'
    # Get the first loaded key
    current_key_fingerprint=$(echo "$ssh_add_output" | head -1 | awk '{print $2}')
    current_key_comment=$(echo "$ssh_add_output" | head -1 | awk '{print $3}')
  else
    # No keys loaded or SSH agent issue
    echo "   None - no key currently active"
    current_key_fingerprint=""
    current_key_comment=""
    
    # If agent isn't running, start it
    [ $ssh_add_exit_code -eq 2 ] && { echo "   Starting SSH agent..."; eval "$(ssh-agent -s)" > /dev/null; }
  fi
  
  # Key selection options
  echo -e "\n┌────────────────────────────────────────┐\n│  PLEASE SELECT A KEY OPTION BELOW:     │\n└────────────────────────────────────────┘"
  echo -e "📌 KEY ACTIVATION OPTIONS:\n   • Press ENTER to keep current key (if any)\n   • Enter a number between 1-${#ALL_SSH_KEYS[@]} to activate that specific key\n   • Enter 'C' to clear all keys and start fresh\n"
  echo -n "Your choice [default=keep current]: "
  read key_choice
  echo ""
  
  # Function to activate a key and verify it
  activate_key() {
    local key="$1" key_name=$(basename "$1")
    echo "🔄 Clearing existing keys from agent..."
    ssh-add -D > /dev/null
    echo "🔑 Activating key: $key_name"
    
    if ssh-add "$key" 2>/dev/null || ssh-add "$key"; then
      echo "✅ Key activated successfully"
      ACTIVE_KEY_PATH="$key"
      ACTIVE_KEY_NAME="$key_name"
      echo -e "\nVerifying key activation..."
      verify_single_key "$key"
      return 0
    else
      echo "❌ Failed to activate key"
      return 1
    fi
  }
  
  if [[ "$key_choice" =~ ^[0-9]+$ ]] && [ "$key_choice" -ge 1 ] && [ "$key_choice" -le "${#ALL_SSH_KEYS[@]}" ]; then
    # User selected a specific key by number
    activate_key "${ALL_SSH_KEYS[$((key_choice-1))]}" && return 0 || { echo "   Please select a different key"; return 1; }
    
  elif [[ "$key_choice" =~ ^[Cc]$ ]]; then
    # Clear all keys and restart the selection process
    echo "🔄 Clearing all keys from agent..."
    ssh-add -D > /dev/null
    echo "✅ All keys cleared"
    echo -e "\nPress ENTER to select a new key..."
    read
    return 1  # Try again - loop back
    
  else
    # Keep current key (default)
    if [ -n "$current_key_fingerprint" ]; then
      echo "🔄 Keeping current key: $current_key_comment"
      debug_log "Key selected: current key ($current_key_comment)"
      
      # Find the key path for later use
      local current_key_path=""
      for i in "${!ALL_SSH_KEYS[@]}"; do
        key="${ALL_SSH_KEYS[$i]}"
        comment="${KEY_COMMENTS[$i]}"
        
        # Match by comment if available or by key filename
        if ([ -n "$comment" ] && [ "$comment" == "$current_key_comment" ]) || 
           [[ "$current_key_comment" == *"$(basename "$key")"* ]] || 
           [[ "$(basename "$key")" == *"$current_key_comment"* ]]; then
          current_key_path="$key"
          debug_log "Matched key: $key"
          break
        fi
      done
      
      # Set global variables to use later
      ACTIVE_KEY_PATH="$current_key_path"
      ACTIVE_KEY_NAME="$current_key_comment"
      
      # Verify only one key is active
      echo -e "\nVerifying key activation..."
      verify_single_key "$current_key_path"
      
      return 0  # Success
    else
      echo "⚠️ No key currently active - please select one"
      debug_log "No active key found, requiring selection"
      
      # Force the user to select a key since none is active
      echo -e "\n👉 Please select a key number (1-${#ALL_SSH_KEYS[@]}):"
      echo -n "Enter key number: "
      read forced_key_num
      
      if [[ "$forced_key_num" =~ ^[0-9]+$ ]] && [ "$forced_key_num" -ge 1 ] && [ "$forced_key_num" -le "${#ALL_SSH_KEYS[@]}" ]; then
        activate_key "${ALL_SSH_KEYS[$((forced_key_num-1))]}" && return 0 || return 1
      else
        echo "❌ Invalid key number"
        debug_log "Invalid forced key selection: $forced_key_num" 
        return 1  # Try again
      fi
    fi
  fi
}

################################
# MAIN SCRIPT EXECUTION
################################

clear
echo -e "┌───────────────────────────────────────────┐\n│     🔐 GIT SSH AUTHENTICATION SETUP       │\n└───────────────────────────────────────────┘\n"

# Helper variables
ACTIVE_KEY_PATH=""
ACTIVE_KEY_NAME=""

# Scan for SSH keys once and use throughout the script
declare -a ALL_SSH_KEYS=()    # Key paths
declare -a KEY_COMMENTS=()    # Key comments (usually emails)
declare -a KEY_EMAILS=()      # Extracted emails
declare -a KEY_NAMES=()       # Extracted names

# Function to scan SSH keys
function scan_ssh_keys() {
  echo "🔍 Scanning for SSH identities..."
  debug_log "SSH key search pattern: ~/.ssh/id_*"
  
  # Count private SSH keys
  ssh_key_count=0
  [ -d ~/.ssh ] && {
    for key_path in ~/.ssh/id_*; do
      [[ -f "$key_path" && ! "$key_path" =~ \.pub$ && ! "$key_path" =~ \.bak$ ]] && ((ssh_key_count++))
    done
  }
  
  debug_log "Found $ssh_key_count SSH private keys"
  
  # Handle case where no keys are found
  if [ $ssh_key_count -eq 0 ]; then
    debug_log "No SSH keys found matching pattern"
    # Debug info and create .ssh if needed
    [ -d ~/.ssh ] && debug_log "Contents of ~/.ssh directory: $(ls -la ~/.ssh 2>/dev/null)" || 
      { debug_log "~/.ssh directory does not exist"; echo "Creating ~/.ssh directory..."; mkdir -p ~/.ssh && chmod 700 ~/.ssh; }
    
    echo -e "❌ No SSH keys found\nSSH keys must be available on your host machine.\n"
    echo -e "Options:\n1. Generate keys on host with: ssh-keygen -t ed25519 -C \"your_email@example.com\"\n2. Ensure your keys are in the ~/.ssh directory\n3. Return to this script after keys are available\n"
    read -p "Press Enter to exit..."
    exit 1
  fi
  
  # Process each SSH private key
  for key in ~/.ssh/id_*; do
    # Skip if it's not a file or is a public key
    [[ ! -f "$key" || "$key" =~ \.pub$ || "$key" =~ \.bak$ ]] && { debug_log "Skipping non-private key file: $key"; continue; }
    
    debug_log "Processing key: $key"
    local pub_key="${key}.pub" key_comment="" email="" name=""
    
    # Get comment from public key if available
    if [ -f "$pub_key" ]; then
      debug_log "Public key found for $key"
      key_comment=$(cat "$pub_key" | awk '{print $3}')
      
      # Extract email and name if comment looks like an email
      [[ "$key_comment" == *"@"* ]] && {
        email="$key_comment"
        name=$(echo "$email" | cut -d@ -f1 | sed 's/\./\ /g' | sed 's/\<./\U&/g')
        debug_log "Email: $email, Name: $name"
      }
    fi
    
    # Store key information in arrays
    ALL_SSH_KEYS+=("$key")
    KEY_COMMENTS+=("$key_comment")
    KEY_EMAILS+=("$email")
    KEY_NAMES+=("$name")
    debug_log "Added key to arrays - path: $key, comment: $key_comment"
  done
  
  debug_log "Scan complete - found ${#ALL_SSH_KEYS[@]} SSH keys"
}

# Debug output of keys and their identities
if [ "$DEBUG" = "1" ]; then
  debug_log() {
    echo "DEBUG: $1" >&2
  }
  # Display debug info directly in debug_log() calls
  scan_ssh_keys
  debug_log "Found ${#ALL_SSH_KEYS[@]} SSH keys"
  for i in "${!ALL_SSH_KEYS[@]}"; do
    debug_log "Key $((i+1)): ${ALL_SSH_KEYS[$i]}"
    debug_log "  Comment: ${KEY_COMMENTS[$i]}"
    debug_log "  Email: ${KEY_EMAILS[$i]}"
    debug_log "  Name: ${KEY_NAMES[$i]}"
  done
else
  # For non-debug mode, run scan_ssh_keys normally
  scan_ssh_keys
fi

#######################################
# SECTION 1: GIT IDENTITY CONFIGURATION
#######################################
echo "STEP 1: Checking Git identity..."

if [ -z "$(git config --global user.name)" ] || [ -z "$(git config --global user.email)" ]; then
  echo "❌ Git identity not configured in container"
  
  # Check if we've extracted any emails from keys
  valid_identities=0
  for email in "${KEY_EMAILS[@]}"; do
    [ -n "$email" ] && ((valid_identities++))
  done
  
  if [ $valid_identities -gt 0 ]; then
    echo "🔍 Auto-detected Git identities from SSH keys:"
    count=0
    for i in "${!KEY_EMAILS[@]}"; do
      email="${KEY_EMAILS[$i]}"
      [ -n "$email" ] && { 
        ((count++))
        echo "   $count) ${KEY_NAMES[$i]} <$email> ($(basename "${ALL_SSH_KEYS[$i]}"))"
      }
    done
    
    echo -e "\nOptions:"
    echo "   • Enter a number (1-$count) to use that identity"
    echo "   • Enter 'M' to manually configure identity"
    echo "   • Enter 'S' to skip (not recommended)"
    read -p "Select identity [1]: " identity_choice
    
    # Default to first identity if empty
    identity_choice=${identity_choice:-1}
    
    # Find the selected identity
    if [[ "$identity_choice" =~ ^[0-9]+$ ]] && [ "$identity_choice" -ge 1 ] && [ "$identity_choice" -le "$count" ]; then
      # Find the corresponding identity
      selected_idx=-1
      count=0
      for i in "${!KEY_EMAILS[@]}"; do
        email="${KEY_EMAILS[$i]}"
        if [ -n "$email" ]; then
          ((count++))
          [ $count -eq $identity_choice ] && { selected_idx=$i; break; }
        fi
      done
      
      if [ $selected_idx -ge 0 ]; then
        # Use selected identity
        git_username="${KEY_NAMES[$selected_idx]}"
        git_email="${KEY_EMAILS[$selected_idx]}"
        echo "✅ Selected identity: $git_username <$git_email>"
      else
        echo "❌ Error finding selected identity"
        read -p "Enter your Git user name: " git_username
        read -p "Enter your Git email address: " git_email
      fi
    elif [[ "$identity_choice" =~ ^[Mm]$ ]]; then
      # Manual configuration
      read -p "Enter your Git user name: " git_username
      read -p "Enter your Git email address: " git_email
    elif [[ "$identity_choice" =~ ^[Ss]$ ]]; then
      # Skip configuration
      echo "⚠️ Skipping Git identity configuration"
      git_username=""
      git_email=""
    else
      echo "❌ Invalid selection"
      read -p "Enter your Git user name: " git_username
      read -p "Enter your Git email address: " git_email
    fi
  else
    echo "❓ No Git identities detected from SSH keys"
    echo -e "\nOptions:"
    echo "  Y - Configure Git identity now"
    echo "  N - Skip (not recommended)"
    read -p "Configure Git identity? [Y/n]: " setup_git
    
    if [[ ! "$setup_git" =~ ^[Nn]$ ]]; then
      read -p "Enter your Git user name: " git_username
      read -p "Enter your Git email address: " git_email
    else
      echo "⚠️ Skipping Git identity configuration"
      git_username=""
      git_email=""
    fi
  fi
  
  # Configure Git if values are provided
  if [ -n "$git_username" ] && [ -n "$git_email" ]; then
    git config --global user.name "$git_username"
    git config --global user.email "$git_email"
    echo "✅ Git identity configured"
  fi
else
  echo "✅ Git identity detected: $(git config --global user.name) <$(git config --global user.email)>"
fi

echo ""

#######################################
# SECTION 2: SSH CONFIG FIXES
#######################################
echo "STEP 2: Checking SSH configuration..."

# Fix SSH config issues
SSH_CONFIG_FIXED=false

# Fix UseKeychain option (not supported in Linux containers)
if [ -f ~/.ssh/config ] && grep -q "UseKeychain" ~/.ssh/config; then
  echo "🔧 Fixing SSH config: removing macOS-specific UseKeychain option..."
  cp ~/.ssh/config ~/.ssh/config.bak # Create a backup
  sed -i '/UseKeychain/d' ~/.ssh/config
  SSH_CONFIG_FIXED=true
fi

# Fix macOS paths in SSH config
if [ -f ~/.ssh/config ] && grep -q "/Users/" ~/.ssh/config; then
  echo "🔧 Fixing SSH config: adjusting macOS paths to container paths..."
  sed -i 's|/Users/[^/]*/\.ssh/|~/.ssh/|g' ~/.ssh/config
  SSH_CONFIG_FIXED=true
fi

# Report on SSH config status
echo "✅ $([[ "$SSH_CONFIG_FIXED" = true ]] && echo "SSH config fixed (backup saved at ~/.ssh/config.bak)" || echo "SSH config looks good")"

# Start SSH agent if not running
if ! ssh-add -l &>/dev/null; then
  echo "🔄 Starting SSH agent..."
  eval "$(ssh-agent -s)"
else
  echo "✅ SSH agent already running"
fi

echo ""

#######################################
# SECTION 3: SSH KEY MANAGEMENT
#######################################
# Global variables to store key information
ACTIVE_KEY_PATH=""
ACTIVE_KEY_NAME=""

# Keep trying until we get a successful key setup
while true; do
  manage_ssh_keys
  [ $? -eq 0 ] && break  # Success - we have an active key
  [ $? -eq 2 ] && { echo "Exiting due to fatal error"; exit 1; }  # Fatal error
  # If result is 1, loop will continue
done

# Show final key selection
echo -e "\n📋 Current active SSH key:"
updated_keys=$(ssh-add -l 2>/dev/null)
if [ $? -eq 0 ] && [ -n "$updated_keys" ]; then
  echo "$updated_keys" | sed 's/^/   /'
  
  # Verify ACTIVE_KEY_NAME is set
  if [ -z "$ACTIVE_KEY_NAME" ]; then
    echo "⚠️ Active key name not captured, using first key"
    ACTIVE_KEY_NAME=$(echo "$updated_keys" | head -1 | awk '{print $3}')
  fi
else
  echo "   Error: No SSH key active"
  echo "❌ No SSH key active - cannot continue"
  exit 1
fi

echo ""

#######################################
# SECTION 4: GITHUB SSH CONFIGURATION
#######################################
echo "STEP 4: Configuring GitHub SSH access..."

# GitHub SSH configuration
if [ -n "$ACTIVE_KEY_NAME" ]; then
  echo "Performing final verification before GitHub configuration..."
  
  # Force clearing all keys and readding only the selected key
  echo "🔄 Final check: ensuring only the selected key is active..."
  ssh-add -D > /dev/null
  if [ -n "$ACTIVE_KEY_PATH" ]; then
    echo "🔑 Activating ONLY: $ACTIVE_KEY_NAME"
    ssh-add "$ACTIVE_KEY_PATH" &>/dev/null || ssh-add "$ACTIVE_KEY_PATH"
    
    # Display currently active keys
    echo "📋 Verifying current active SSH keys:"
    ssh-add -l | sed 's/^/   /'
    
    # Count keys to verify
    num_active_keys=$(ssh-add -l 2>/dev/null | grep -E "SHA256|MD5" | wc -l)
    [ "$num_active_keys" -ne 1 ] && 
      echo "⚠️ Warning: Found $num_active_keys active keys instead of exactly 1" || 
      echo "✅ Verification successful: exactly 1 key active"
  fi
  
  echo "✅ Using active SSH key for GitHub: $ACTIVE_KEY_NAME"
  
  # Create GitHub-specific SSH config entry with current key
  mkdir -p ~/.ssh
  cat >> ~/.ssh/config << EOF

# Added by setup-git-ssh.sh
Host github.com
  HostName github.com
  User git
  IdentitiesOnly yes
  IdentityAgent SSH_AUTH_SOCK
EOF
  echo "✅ GitHub SSH config updated to use current key"
else
  echo "❌ No SSH key currently active"
  echo "   Please run this script again and activate a key"
  exit 1
fi

#######################################
# SECTION 5: TESTING & VERIFICATION
#######################################
echo "STEP 5: Testing & verification..."

# Test SSH connection to GitHub
echo "🔄 Testing SSH connection to GitHub..."
debug_log "Running SSH test with: git@github.com"

# Capture output and exit code for better diagnosis
ssh_test_output=$(ssh -T git@github.com -o BatchMode=yes -o StrictHostKeyChecking=accept-new 2>&1)
ssh_test_exit_code=$?

debug_log "SSH test exit code: $ssh_test_exit_code"
debug_log "SSH test raw output: $ssh_test_output"

# Check for successful authentication
if echo "$ssh_test_output" | grep -q "successfully authenticated"; then
  echo "✅ SSH connection to GitHub successful!"
else
  echo -e "⚠️ SSH connection test returned unexpected result\n"
  echo "📋 Output from SSH test:"
  
  # Show SSH test output with optional timeout
  if [ -n "$ssh_test_output" ]; then
    echo "$ssh_test_output"
  else
    echo -e "   No output received from SSH test (possible timeout)\n   Running interactive test:"
    debug_log "Running interactive SSH test"
    ssh -Tv git@github.com -o StrictHostKeyChecking=accept-new
  fi

  echo -e "\n⚠️ If connection failed, try running with DEBUG=1 for verbose output:"
  echo "   DEBUG=1 .devcontainer/setup-git-ssh.sh"
fi

# Configure Git to prefer SSH
echo -e "\n🔄 Configuring Git to use SSH for GitHub repositories..."
git config --global url."git@github.com:".insteadOf "https://github.com/"
echo "✅ Git configured to use SSH for GitHub URLs"

echo -e "\n┌───────────────────────────────────────────┐"
echo "│     🎉 GIT SSH SETUP COMPLETE!            │"
echo "└───────────────────────────────────────────┘"
echo -e "\nYou can now use Git with SSH authentication:"
echo "  $ git clone git@github.com:username/repo.git"
echo "  $ git push origin main"
echo -e "\nFor troubleshooting, run: DEBUG=1 .devcontainer/setup-git-ssh.sh"
echo "" 