# Tag Scanner v.1

This Python library searches through a static directory (needs to match your environment) and appends artist, title, track number, album title, duration, and genre to a .json object. This .json object is then used to post data to a specified table in a local MySQL database, credentials of which the user must set. 

# Usage

While in directory:

```
sudo apt-get install virtualenv

source venv/bin/activate

python mp3_reader.py
```

# Libraries Used

Mutagen, PyMySQL, virtualenv
