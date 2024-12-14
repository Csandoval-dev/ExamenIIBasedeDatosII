import chardet

with open('Carros.txt', 'rb') as f:
    result = chardet.detect(f.read())
    print(result)
