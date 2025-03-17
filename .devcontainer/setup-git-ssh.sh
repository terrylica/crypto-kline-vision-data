#!/bin/bash

# Disable "exit on error" for our interactive script
set +e

################################
# CONFIG AND HELPER FUNCTIONS
################################

DEBUG="${DEBUG:-0}"
[ "$DEBUG" = "1" ] && echo "DEBUG MODE ENABLED"

function debug_log() { [ "$DEBUG" = "1" ] && echo "DEBUG: $1" >&2; }

function print_msg() {
  local type="$1" msg="$2"
  case "$type" in 
    header)     clear; echo -e "┌───────────────────────────────────────────┐\n│     🔐 GIT SSH AUTHENTICATION SETUP       │\n└───────────────────────────────────────────┘\n"; [ -n "$msg" ] && echo "$msg" ;;
    section)    echo -e "\n┌────────────────────────────────────────┐\n│  $msg     │\n└────────────────────────────────────────┘" ;;
    success)    echo "✅ $msg" ;;
    warning)    echo "⚠️ $msg" ;;
    error)      echo "❌ $msg" ;;
    info)       echo "🔍 $msg" ;;
    action)     echo "🔄 $msg" ;;
    key)        echo "🔑 $msg" ;;
    *)          echo "$msg" ;;
  esac
}

# SSH operations
function ensure_ssh_agent_running() {
  if ! ssh-add -l &>/dev/null; then
    print_msg action "Starting SSH agent..."
    eval "$(ssh-agent -s)" > /dev/null
    return 0
  else return 1; fi
}

function ssh_key_ops() {
  local op="$1" arg="$2"
  case "$op" in
    list)   ssh-add -l 2>/dev/null ;;
    count)  ssh-add -l 2>/dev/null | grep -E "SHA256|MD5" | wc -l ;;
    clear)  print_msg action "Clearing all keys from agent..."; ssh-add -D > /dev/null; print_msg success "All keys cleared" ;;
    add)    local key="$arg" key_name=$(basename "$arg")
            ssh_key_ops clear
            print_msg key "Activating key: $key_name"
            if ssh-add "$key" 2>/dev/null || ssh-add "$key"; then
              print_msg success "Key activated successfully"
              ACTIVE_KEY_PATH="$key"; ACTIVE_KEY_NAME="$key_name"
              return 0
            else print_msg error "Failed to activate key"; return 1; fi ;;
  esac
}

################################
# SSH KEY MANAGEMENT FUNCTION
################################

function verify_single_key() {
  # Function to verify only one key is active and fix if needed
  local num_keys=$(ssh_key_ops count)
  
  if [ "$num_keys" -eq 0 ]; then
    print_msg warning "No SSH keys currently active"
    return 1
  elif [ "$num_keys" -gt 1 ]; then
    print_msg warning "Multiple keys detected ($num_keys keys active)"
    ssh_key_ops list | sed 's/^/   /'
    
    # Clear all keys and re-add just the selected key
    ssh_key_ops clear
    
    if [ -n "$1" ]; then
      print_msg key "Reactivating only key: $(basename "$1")"
      if ssh-add "$1" &>/dev/null || ssh-add "$1"; then
        # Double-check that we only have one key now
        [ "$(ssh_key_ops count)" -eq 1 ] && 
          print_msg success "Successfully activated single key" ||
          { print_msg warning "Still detected multiple keys after cleanup attempt"; ssh_key_ops clear; sleep 1; ssh-add "$1" &>/dev/null || ssh-add "$1"; }
        return 0
      else
        print_msg error "Failed to reactivate key"
        return 1
      fi
    else
      print_msg error "No key path provided for reactivation"
      return 1
    fi
  else
    print_msg success "Verified: Single key active"
    return 0
  fi
}

function manage_ssh_keys() {
  # This function handles scanning, displaying, and activating SSH keys
  # Returns: 0 = success, 1 = try again, 2 = fatal error
  
  print_msg header "STEP 3: Managing SSH keys..."
  
  # Debug
  [ "$DEBUG" = "1" ] && set -x
  
  # Use the pre-scanned SSH keys from the global arrays
  if [ ${#ALL_SSH_KEYS[@]} -eq 0 ]; then
    print_msg error "No SSH keys found in ~/.ssh directory"
    echo "   You may need to set up SSH keys on your host machine"
    return 2  # Fatal error
  fi
  
  # Display available SSH keys
  print_msg info "Available SSH keys:"
  for i in "${!ALL_SSH_KEYS[@]}"; do
    key="${ALL_SSH_KEYS[$i]}"
    key_name=$(basename "$key")
    comment="${KEY_COMMENTS[$i]}"
    
    echo "   $((i+1))) $key_name${comment:+ ($comment)}"
  done
  
  # Show currently loaded keys
  echo -e "\n📋 Currently active SSH key:"
  
  # Capture ssh-add output and exit code
  local ssh_add_output=$(ssh_key_ops list)
  local ssh_add_exit_code=$?
  local current_key_fingerprint="" current_key_comment=""
  
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
    # If agent isn't running, start it
    [ $ssh_add_exit_code -eq 2 ] && ensure_ssh_agent_running
  fi
  
  # Key selection options
  print_msg section "PLEASE SELECT A KEY OPTION BELOW:"
  echo -e "📌 KEY ACTIVATION OPTIONS:\n   • Press ENTER to keep current key (if any)\n   • Enter a number between 1-${#ALL_SSH_KEYS[@]} to activate that specific key\n   • Enter 'C' to clear all keys and start fresh\n"
  echo -n "Your choice [default=keep current]: "
  read key_choice
  echo ""
  
  if [[ "$key_choice" =~ ^[0-9]+$ ]] && [ "$key_choice" -ge 1 ] && [ "$key_choice" -le "${#ALL_SSH_KEYS[@]}" ]; then
    # User selected a specific key by number
    ssh_key_ops add "${ALL_SSH_KEYS[$((key_choice-1))]}" && return 0 || { echo "   Please select a different key"; return 1; }
    
  elif [[ "$key_choice" =~ ^[Cc]$ ]]; then
    # Clear all keys and restart the selection process
    ssh_key_ops clear
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
      print_msg warning "No key currently active - please select one"
      debug_log "No active key found, requiring selection"
      
      # Force the user to select a key since none is active
      echo -e "\n👉 Please select a key number (1-${#ALL_SSH_KEYS[@]}):"
      echo -n "Enter key number: "
      read forced_key_num
      
      if [[ "$forced_key_num" =~ ^[0-9]+$ ]] && [ "$forced_key_num" -ge 1 ] && [ "$forced_key_num" -le "${#ALL_SSH_KEYS[@]}" ]; then
        ssh_key_ops add "${ALL_SSH_KEYS[$((forced_key_num-1))]}" && return 0 || return 1
      else
        print_msg error "Invalid key number"
        debug_log "Invalid forced key selection: $forced_key_num" 
        return 1  # Try again
      fi
    fi
  fi
}

################################
# MAIN SCRIPT EXECUTION
################################

print_msg header

# Helper variables
ACTIVE_KEY_PATH=""
ACTIVE_KEY_NAME=""

# Scan for SSH keys once and use throughout the script
declare -a ALL_SSH_KEYS=() KEY_COMMENTS=() KEY_EMAILS=() KEY_NAMES=()

# Function to scan SSH keys
function scan_ssh_keys() {
  print_msg info "Scanning for SSH identities..."
  debug_log "SSH key search pattern: ~/.ssh/id_*"
  
  # Count private SSH keys
  local ssh_key_count=0
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
    
    print_msg error "No SSH keys found\nSSH keys must be available on your host machine.\n"
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
    ALL_SSH_KEYS+=("$key"); KEY_COMMENTS+=("$key_comment")
    KEY_EMAILS+=("$email"); KEY_NAMES+=("$name")
    debug_log "Added key to arrays - path: $key, comment: $key_comment"
  done
  
  debug_log "Scan complete - found ${#ALL_SSH_KEYS[@]} SSH keys"
}

# Run scan_ssh_keys with debug support if needed
if [ "$DEBUG" = "1" ]; then
  scan_ssh_keys
  debug_log "Found ${#ALL_SSH_KEYS[@]} SSH keys"
  for i in "${!ALL_SSH_KEYS[@]}"; do
    debug_log "Key $((i+1)): ${ALL_SSH_KEYS[$i]}, Comment: ${KEY_COMMENTS[$i]}, Email: ${KEY_EMAILS[$i]}, Name: ${KEY_NAMES[$i]}"
  done
else
  scan_ssh_keys
fi

#######################################
# SECTION 1: GIT IDENTITY CONFIGURATION
#######################################
echo "STEP 1: Checking Git identity..."

function configure_git_identity() {
  if [ -n "$1" ] && [ -n "$2" ]; then
    git config --global user.name "$1"
    git config --global user.email "$2"
    print_msg success "Git identity configured"
  fi
}

if [ -z "$(git config --global user.name)" ] || [ -z "$(git config --global user.email)" ]; then
  print_msg error "Git identity not configured in container"
  
  # Check if we've extracted any emails from keys
  local valid_identities=0 git_username="" git_email=""
  for email in "${KEY_EMAILS[@]}"; do [ -n "$email" ] && ((valid_identities++)); done
  
  if [ $valid_identities -gt 0 ]; then
    print_msg info "Auto-detected Git identities from SSH keys:"
    local count=0
    for i in "${!KEY_EMAILS[@]}"; do
      email="${KEY_EMAILS[$i]}"
      [ -n "$email" ] && { 
        ((count++))
        echo "   $count) ${KEY_NAMES[$i]} <$email> ($(basename "${ALL_SSH_KEYS[$i]}"))"
      }
    done
    
    echo -e "\nOptions:\n   • Enter a number (1-$count) to use that identity\n   • Enter 'M' to manually configure identity\n   • Enter 'S' to skip (not recommended)"
    read -p "Select identity [1]: " identity_choice
    
    # Default to first identity if empty
    identity_choice=${identity_choice:-1}
    
    # Find the selected identity
    if [[ "$identity_choice" =~ ^[0-9]+$ ]] && [ "$identity_choice" -ge 1 ] && [ "$identity_choice" -le "$count" ]; then
      # Find the corresponding identity
      local selected_idx=-1 count=0
      for i in "${!KEY_EMAILS[@]}"; do
        email="${KEY_EMAILS[$i]}"
        if [ -n "$email" ]; then
          ((count++))
          [ $count -eq $identity_choice ] && { selected_idx=$i; break; }
        fi
      done
      
      if [ $selected_idx -ge 0 ]; then
        git_username="${KEY_NAMES[$selected_idx]}"
        git_email="${KEY_EMAILS[$selected_idx]}"
        print_msg success "Selected identity: $git_username <$git_email>"
      else
        print_msg error "Error finding selected identity"
        read -p "Enter your Git user name: " git_username
        read -p "Enter your Git email address: " git_email
      fi
    elif [[ "$identity_choice" =~ ^[Mm]$ ]]; then
      read -p "Enter your Git user name: " git_username
      read -p "Enter your Git email address: " git_email
    elif [[ "$identity_choice" =~ ^[Ss]$ ]]; then
      print_msg warning "Skipping Git identity configuration"
    else
      print_msg error "Invalid selection"
      read -p "Enter your Git user name: " git_username
      read -p "Enter your Git email address: " git_email
    fi
  else
    echo "❓ No Git identities detected from SSH keys"
    echo -e "\nOptions:\n  Y - Configure Git identity now\n  N - Skip (not recommended)"
    read -p "Configure Git identity? [Y/n]: " setup_git
    
    if [[ ! "$setup_git" =~ ^[Nn]$ ]]; then
      read -p "Enter your Git user name: " git_username
      read -p "Enter your Git email address: " git_email
    else
      print_msg warning "Skipping Git identity configuration"
    fi
  fi
  
  # Configure Git if values are provided
  configure_git_identity "$git_username" "$git_email"
else
  print_msg success "Git identity detected: $(git config --global user.name) <$(git config --global user.email)>"
fi

echo ""

#######################################
# SECTION 2: SSH CONFIG FIXES
#######################################
echo "STEP 2: Checking SSH configuration..."

function fix_ssh_config() {
  local fixed=false do_backup=true
  
  # Fix UseKeychain option and macOS paths in SSH config
  if [ -f "$1" ]; then
    if grep -q "UseKeychain" "$1"; then
      [ "$do_backup" = true ] && { cp "$1" "${1}.bak"; do_backup=false; }
      print_msg action "Fixing SSH config: removing macOS-specific UseKeychain option..."
      sed -i '/UseKeychain/d' "$1"
      fixed=true
    fi
    
    if grep -q "/Users/" "$1"; then
      [ "$do_backup" = true ] && { cp "$1" "${1}.bak"; do_backup=false; }
      print_msg action "Fixing SSH config: adjusting macOS paths to container paths..."
      sed -i 's|/Users/[^/]*/\.ssh/|~/.ssh/|g' "$1"
      fixed=true
    fi
  fi
  
  echo "$fixed"
}

# Fix SSH config issues and start SSH agent if needed
SSH_CONFIG_FIXED=$(fix_ssh_config ~/.ssh/config)
print_msg success "$([[ "$SSH_CONFIG_FIXED" = true ]] && echo "SSH config fixed (backup saved at ~/.ssh/config.bak)" || echo "SSH config looks good")"
ensure_ssh_agent_running && print_msg success "SSH agent started" || print_msg success "SSH agent already running"
echo ""

#######################################
# SECTION 3: SSH KEY MANAGEMENT
#######################################
# Keep trying until we get a successful key setup
while true; do
  manage_ssh_keys
  code=$?
  [ $code -eq 0 ] && break  # Success - we have an active key
  [ $code -eq 2 ] && { echo "Exiting due to fatal error"; exit 1; }  # Fatal error
  # If result is 1, loop will continue
done

# Show final key selection
echo -e "\n📋 Current active SSH key:"
updated_keys=$(ssh_key_ops list)
if [ $? -eq 0 ] && [ -n "$updated_keys" ]; then
  echo "$updated_keys" | sed 's/^/   /'
  
  # Verify ACTIVE_KEY_NAME is set
  if [ -z "$ACTIVE_KEY_NAME" ]; then
    print_msg warning "Active key name not captured, using first key"
    ACTIVE_KEY_NAME=$(echo "$updated_keys" | head -1 | awk '{print $3}')
  fi
else
  echo "   Error: No SSH key active"
  print_msg error "No SSH key active - cannot continue"
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
  print_msg action "Final check: ensuring only the selected key is active..."
  ssh_key_ops clear
  
  if [ -n "$ACTIVE_KEY_PATH" ]; then
    print_msg key "Activating ONLY: $ACTIVE_KEY_NAME"
    ssh-add "$ACTIVE_KEY_PATH" &>/dev/null || ssh-add "$ACTIVE_KEY_PATH"
    
    # Display currently active keys and verify
    echo "📋 Verifying current active SSH keys:"
    ssh_key_ops list | sed 's/^/   /'
    
    num_active_keys=$(ssh_key_ops count)
    [ "$num_active_keys" -ne 1 ] && 
      print_msg warning "Warning: Found $num_active_keys active keys instead of exactly 1" || 
      print_msg success "Verification successful: exactly 1 key active"
  fi
  
  print_msg success "Using active SSH key for GitHub: $ACTIVE_KEY_NAME"
  
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
  print_msg success "GitHub SSH config updated to use current key"
else
  print_msg error "No SSH key currently active"
  echo "   Please run this script again and activate a key"
  exit 1
fi

#######################################
# SECTION 5: TESTING & VERIFICATION
#######################################
echo "STEP 5: Testing & verification..."

# Test SSH connection to GitHub
print_msg action "Testing SSH connection to GitHub..."
debug_log "Running SSH test with: git@github.com"

ssh_test_output=$(ssh -T git@github.com -o BatchMode=yes -o StrictHostKeyChecking=accept-new 2>&1)
ssh_test_exit_code=$?

debug_log "SSH test exit code: $ssh_test_exit_code"
debug_log "SSH test raw output: $ssh_test_output"

# Check results and provide feedback
if echo "$ssh_test_output" | grep -q "successfully authenticated"; then
  print_msg success "SSH connection to GitHub successful!"
else
  echo -e "⚠️ SSH connection test returned unexpected result\n"
  echo "📋 Output from SSH test:"
  
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
print_msg action "Configuring Git to use SSH for GitHub repositories..."
git config --global url."git@github.com:".insteadOf "https://github.com/"
print_msg success "Git configured to use SSH for GitHub URLs"

echo -e "\n┌───────────────────────────────────────────┐"
echo "│     🎉 GIT SSH SETUP COMPLETE!            │"
echo "└───────────────────────────────────────────┘"
echo -e "\nYou can now use Git with SSH authentication:"
echo "  $ git clone git@github.com:username/repo.git"
echo "  $ git push origin main"
echo -e "\nFor troubleshooting, run: DEBUG=1 .devcontainer/setup-git-ssh.sh"
echo "" 