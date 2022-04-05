FROM golang
# We copy everything into an app directory
WORKDIR /app
COPY . .
# we run go build to compile the binary
# executable of our Go program
RUN go build -o main .
# Our start command which kicks off
# our newly created binary executable
CMD "/app/main"