#!/usr/bin/env python
"""Hash the passwords of a `username:password` file in place.

The default format is scrypt, written as

    $scrypt$<N>$<r>$<p>$<salt hex>$<hash hex>

which is memory hard and therefore much more expensive to attack offline than
the SHA-512 scheme most systems pick for crypt(3). The broker understands both
this format and the crypt(3) one, so `--crypt` is kept for existing files and
for platforms whose crypt is what the operator already uses.
"""

import argparse
import hashlib
import os
import sys

try:
    import crypt
except ImportError:  # pragma: no cover - Windows has no crypt(3)
    crypt = None

SCRYPT_N = 16384
SCRYPT_R = 8
SCRYPT_P = 1
SCRYPT_DKLEN = 64
SALT_BYTES = 16


def hash_scrypt(password):
    salt = os.urandom(SALT_BYTES)
    digest = hashlib.scrypt(password.encode('utf-8'), salt=salt,
                            n=SCRYPT_N, r=SCRYPT_R, p=SCRYPT_P,
                            dklen=SCRYPT_DKLEN)
    return '$scrypt${}${}${}${}${}'.format(
        SCRYPT_N, SCRYPT_R, SCRYPT_P,
        salt.hex(), digest.hex())


def hash_crypt(password):
    if crypt is None:
        raise SystemExit('crypt(3) is not available on this platform, '
                         'use the default scrypt format')
    return crypt.crypt(password)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('password_file',
                        help='file of username:password lines, rewritten '
                             'in place with the hashed passwords')
    parser.add_argument('--crypt', action='store_true',
                        help='use crypt(3) instead of scrypt')
    args = parser.parse_args()

    auths = {}
    with open(args.password_file) as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if ':' not in line:
                print('skipping malformed line: %r' % line, file=sys.stderr)
                continue
            username, password = line.split(':', 1)
            if not username:
                print('skipping a line with an empty username',
                      file=sys.stderr)
                continue
            auths[username] = hash_crypt(password) if args.crypt \
                else hash_scrypt(password)

    with open(args.password_file, 'w') as fh:
        for username, hashed in auths.items():
            fh.write('%s:%s\n' % (username, hashed))


if __name__ == '__main__':
    main()
