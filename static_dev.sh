docker build -f static_dev.Dockerfile -t sqlite-grpsqlite-static .
echo run with:
echo \tdocker run --rm -it sqlite-grpsqlite-static /bin/bash
echo can also use:
echo \tcargo run --example memory_server
echo to run an example server in the container
