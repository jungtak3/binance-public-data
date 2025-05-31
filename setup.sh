#!/bin/bash

# Binance Public Data Downloader Setup Script
echo "Setting up Binance Public Data Downloader..."

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "Python 3 is not installed. Please install Python 3.7 or higher."
    exit 1
fi

# Check if pip is installed
if ! command -v pip3 &> /dev/null; then
    echo "pip3 is not installed. Please install pip3."
    exit 1
fi

# Navigate to python directory
cd python

# Install required packages
echo "Installing required Python packages..."
pip3 install -r requirements.txt

if [ $? -eq 0 ]; then
    echo "✅ Setup completed successfully!"
    echo ""
    echo "You can now run the downloader with:"
    echo "python3 download-kline2.py -t spot -s BTCUSDT -i 1d"
    echo ""
    echo "For more usage examples, see README_SETUP.md"
else
    echo "❌ Setup failed. Please check the error messages above."
    exit 1
fi