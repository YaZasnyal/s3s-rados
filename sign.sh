#! /bin/bash

# Thanks to http://cuz.cx/vince_riv/2013/04/create-signed-s3-url-using-s3cmd/

set -e

usage() {
    echo "$(basename ${0}) -b bucket -o object_uri_part [-a access_key] [-s seconds]"
    echo "- a script to create temporary signed urls for private objects in a S3 bucket."
    echo ""
    echo "Options:"
    echo "  -b bucket_name: S3 Bucket"
    echo "  -o object_uri: URI part of object in S3 bucket - e.g. somefolder/somefile.ext"
    echo "  -m http_method: HTTP method to sign - e.g. PUT (default ${S3_HTTP_METHOD})"
    echo "  -a access_key: S3 Access Key (default ${S3_ACCESS_KEY})"
    echo "  -s seconds: how long the signed url will be valid (default ${SECONDS})"
    echo "  -h: to see this text"
}

set_access_key() {
    # extract access key from .s3cfg
    if [ -e "${HOME}/.s3cfg" ]; then
        S3_ACCESS_KEY=$(sed -n 's/^access_key = //p' "${HOME}/.s3cfg")
    fi
}

create_signed_url() {
    # 1-hour expiration
    TIMESTAMP=$((`date +%s` + ${SECONDS}))
    # string to sign: method + expiration-time + bucket/object
    local can_string="${S3_HTTP_METHOD}\n\n\n${TIMESTAMP}\n/${S3_BUCKET}/${S3_OBJECT}"
    echo $can_string
    # generate the signature
    SIGNATURE=$(s3cmd sign "$(echo -e "$can_string")" | sed -n 's/^Signature: //p')
    if [ -z "$SIGNATURE" ]; then
        echo "Failed to created signed URL for '${S3_OBJECT}' in bucket '${S3_BUCKET}'" >&2
        exit 4
    fi
}

output() {
    local base_url="http://localhost:8014/${S3_BUCKET}"
    local params="AWSAccessKeyId=${S3_ACCESS_KEY}&Expires=${TIMESTAMP}&Signature=${SIGNATURE}"
    echo "$base_url?$params"
}

set_access_key
SECONDS=3600
S3_HTTP_METHOD="GET"

while getopts "hb:o:m:a:s:" OPT; do
    case "$OPT" in
        h)
            usage
            exit 0
            ;;
        b)
            S3_BUCKET="${OPTARG}"
            ;;
        o)
            S3_OBJECT="${OPTARG}"
            ;;
        m)
            S3_HTTP_METHOD="${OPTARG}"
            ;;
        a)
            S3_ACCESS_KEY="${OPTARG}"
            ;;
        s)
            SECONDS="${OPTARG}"
            ;;
    esac
done

if [ -z "$S3_ACCESS_KEY" ]; then
    echo "Please provide your S3 access key."
    exit 1
fi

if [ -z "$S3_BUCKET" ]; then
    echo "Please provide your S3 bucket name."
    exit 2
fi

if [ -z "$S3_OBJECT" ]; then
    echo "Please provide the object you want a signed URL for."
    exit 3
fi

if [ "$S3_HTTP_METHOD" != "GET" -a "$S3_HTTP_METHOD" != "PUT" ]; then
    echo "HTTP method is unknown: '$S3_HTTP_METHOD' (use GET or PUT)"
    exit 4
fi

create_signed_url
output