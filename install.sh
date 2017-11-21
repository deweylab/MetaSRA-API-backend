echo "Setting up python virtual environment"
echo "Python 3.x is required.  You might need to install a separate package for python-venv if you get an error."
rm -rf .ENV # remove .ENV if already present
python3 -m venv .ENV


echo "Installing packages"
source ENV/bin/activate
pip install -r requirements.txt
