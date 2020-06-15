# apt-transport-tardigrade

_A Tardigrade.io transport method for the `apt` package management system_

The apt-transport-tardigrade project provides support for downloading APT
package lists and packages via the [Tardigrade](https://tardigrade.io/)
network. Tardigrade provides inexpensive, reliable, secure, and relatively
fast online storage via a distributed network of individual providers. It
is powered by the [Storj](https://storj.io/) project.

## TL;DR
1. Build the binary `$ go build -o apt-transport-tardigrade main.go`
1. Install the binary `$ sudo cp apt-transport-tardigrade /usr/lib/apt/methods/tardigrade`
1. Add your tardigrade based source to a package list; for example, `$ echo "deb tardigrade://satellite-address:port/[access-string]/debian stable main" > /etc/apt/sources.list.d/tardigrade-mirror.list`
1. Update packages `$ sudo apt-get update`

## Installing in production

The `apt-transport-tardigrade` binary is an executable. To install it, copy
it to `/usr/lib/apt/methods/tardigrade` on your computer.

## How it works

Apt creates a child process using the `/usr/lib/apt/methods/tardigrade` binary and
writes to that process's standard input using a specific protocol. The method
interprets the input, downloads the requested files, and communicates back to
apt by writing to its standard output. The protocol spec is available here:
[http://www.fifi.org/doc/libapt-pkg-doc/method.html/ch2.html](http://www.fifi.org/doc/libapt-pkg-doc/method.html/ch2.html).

## Similar Projects

All of these provide an AWS S3 transport instead of Tardigrade:

* [https://github.com/google/apt-golang-s3](https://github.com/google/apt-golang-s3) (from which this project is derived)
* [https://github.com/kyleshank/apt-transport-s3](https://github.com/kyleshank/apt-transport-s3)
* [https://github.com/brianm/apt-s3](https://github.com/brianm/apt-s3)
* [https://github.com/BashtonLtd/apt-transport-s3](https://github.com/BashtonLtd/apt-transport-s3)
* [https://github.com/lucidsoftware/apt-boto-s3/](https://github.com/lucidsoftware/apt-boto-s3/)
