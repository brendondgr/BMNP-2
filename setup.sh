#!/bin/bash

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Step 1: Create a Python environment if it does not exist
if [ ! -d "venv" ]; then
    echo "Creating Python virtual environment..."
    if command_exists python; then
        echo "Using 'python' to create the virtual environment..."
        python -m venv venv
    elif command_exists python3; then
        echo "Using 'python3' to create the virtual environment..."
        python3 -m venv venv
    else
        echo "Python is not installed. Please install Python and try again."
        exit 1
    fi
else
    echo "Python virtual environment already exists."
fi

# Step 2: Activate the virtual environment
echo "Activating the virtual environment..."
if [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "win32" ]]; then
    source venv/Scripts/activate
else
    source venv/bin/activate
fi

# Step 3: Install the necessary packages from requirements.txt
if [ -f "requirements.txt" ]; then
    echo "Installing required packages..."
    pip install -r requirements.txt
else
    echo "requirements.txt not found. Please provide a requirements.txt file."
    exit 1
fi

# Step 4: Run main.py
echo "Running main.py..."
python main.py

echo "Setup complete."