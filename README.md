# apt-transport-storj

_A Storj.io transport method for the `apt` package management system_

The apt-transport-storj project provides support for downloading APT package
lists and packages via the [Storj](https://storj.io/) network. Storj provides
reliable, secure, and fast online storage via a distributed network of
individual providers. Free mirrors of Debian and Ubuntu are provided by
[Tardigrade.io](https://tardigrade.io/), which provides high-quality satellites
and inexpensive storage on the Storj network.

## TL;DR
1. Build the binary `$ go build`
1. Install the binary `$ sudo cp apt-transport-storj /usr/lib/apt/methods/storj-apt`
1. Add your Storj-based source to a package list; for example, `$ echo "deb storj-apt://satellite-address:port/apiKey/debian stable main" | sudo tee -a /etc/apt/sources.list.d/storj-apt-mirror.list`
1. Update packages `$ sudo apt-get update`

## Installing in production

The `apt-transport-storj` binary is an executable. To install it, copy
it to `/usr/lib/apt/methods/storj-apt` on your computer.

## How it works

Apt creates a child process using the `/usr/lib/apt/methods/storj-apt` binary
and writes to that process's standard input using a specific protocol. The
method interprets the input, downloads the requested files, and communicates
back to apt by writing to its standard output. The protocol spec is available
here:
[http://www.fifi.org/doc/libapt-pkg-doc/method.html/ch2.html](http://www.fifi.org/doc/libapt-pkg-doc/method.html/ch2.html).

## Available repositories

[Tardigrade.io](https://tardigrade.io/) provides free mirrors of Debian and
Ubuntu (all current dists and architectures) at:

    deb storj-apt://12EayRS2V1kEsWESU9QMRseFhdxYxKicsiFmxrsLZHeLUtdps3S@us-central-1.tardigrade.io:7777/178ei7NN2nHMkjAcFyWcX8khPU4qhjjdpGNFu45X52CsVmUBiaD7HU3G7fmzpzTxQo75z9mhzjWybZCCVnFsgdBPLxor6Q79urKqwBQSKPNypdwSgMU7aNAACMT4hL/{BUCKET} {DIST} {SECTIONS}

``{BUCKET}`` is either ``debian`` or ``ubuntu``. ``{DIST}`` and ``{SECTIONS}`` act just as they do for any other APT source: ``{DIST}`` should be replaced with the desired dist (e.g., ``unstable``, ``stable``, ``stretch-backports``, ``groovy``), and ``{SECTIONS}`` indicates which sections of the archive you want included (e.g. ``main contrib non-free`` or ``main universe multiverse``).

## Similar Projects

All of these provide an AWS S3 transport instead of Storj:

* [https://github.com/google/apt-golang-s3](https://github.com/google/apt-golang-s3) (from which this project is derived)
* [https://github.com/kyleshank/apt-transport-s3](https://github.com/kyleshank/apt-transport-s3)
* [https://github.com/brianm/apt-s3](https://github.com/brianm/apt-s3)
* [https://github.com/BashtonLtd/apt-transport-s3](https://github.com/BashtonLtd/apt-transport-s3)
* [https://github.com/lucidsoftware/apt-boto-s3/](https://github.com/lucidsoftware/apt-boto-s3/)
