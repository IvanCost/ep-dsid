import pyspark
sc = pyspark.SparkContext('local[*]')


# python -m venv .venv
# Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process
# ./.venv/Scripts/activate

# Se o pip parar de funcionar de novo:
# curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
# py get-pip.py

txt = sc.textFile('text.txt')
print(txt.count())

python_lines = txt.filter(lambda line: 'python' in line.lower())
print(python_lines.count())
