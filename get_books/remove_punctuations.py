from pathlib import Path
import string
from os import listdir
from os.path import isfile, join

directory_path = "/Users/srinathsureshkumar/Workspace/map-reduce/gutenberg_books"
# directory_path = "/Users/srinathsureshkumar/Workspace/map-reduce/spark-wordcount/src/main/resources/input"
onlyfiles = [join(directory_path, f) for f in listdir(directory_path) if isfile(join(directory_path, f))]

for filename in onlyfiles:
    filepath = Path(filename)
    text = filepath.read_text()
    text = text.translate(str.maketrans('', '', string.punctuation))
    text = text.lower()
    filepath.write_text(text)
print(string.punctuation)