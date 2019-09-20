Siddhi IO GCS
======================================

[![Jenkins Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-io-gcs/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-io-gcs/)
[![GitHub Release](https://img.shields.io/github/release/siddhi-io/siddhi-io-gcs.svg)](https://github.com/siddhi-io/siddhi-io-gcs/releases)
[![GitHub Release Date](https://img.shields.io/github/release-date/siddhi-io/siddhi-io-gcs.svg)](https://github.com/siddhi-io/siddhi-io-gcs/releases)
[![GitHub Open Issues](https://img.shields.io/github/issues-raw/siddhi-io/siddhi-io-gcs.svg)](https://github.com/siddhi-io/siddhi-io-gcs/issues)
[![GitHub Last Commit](https://img.shields.io/github/last-commit/siddhi-io/siddhi-io-gcs.svg)](https://github.com/siddhi-io/siddhi-io-gcs/commits/master)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

The **siddhi-io-gcs extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a> 
that used to receive and publish events via Google Cloud Storage Service. This extension allows users to read/upload events to a GCS bucket.

For information on <a target="_blank" href="https://siddhi.io/">Siddhi</a> and it's features refer <a target="_blank" href="https://siddhi.io/redirect/docs.html">Siddhi Documentation</a>. 


## Dependencies
There are no other dependencies needed for this extension.

## Installation
For installing this extension on various siddhi execution environments refer Siddhi documentation section on <a target="_blank" href="https://siddhi.io/redirect/add-extensions.html">adding extensions</a>.

# Support and Contribution

* We encourage users to ask questions and get support via <a target="_blank" href="https://stackoverflow.com/questions/tagged/siddhi">StackOverflow</a>, make sure to add the `siddhi` tag to the issue for better response.

* If you find any issues related to the extension please report them on <a target="_blank" href="https://github.com/siddhi-io/siddhi-execution-string/issues">the issue tracker</a>.

* For production support and other contribution related information refer <a target="_blank" href="https://siddhi.io/community/">Siddhi Community</a> documentation.

**Note: Following packages were private packaged in this extension as they are not OSGI bundles**
- com.google.* version: 1.90.0 License: Apache 2.0
- org.threeten.bp.* version: 1.3.3 License: BSD 3-clause
- io.opencensus.* version: 0.21.0 License: Apache 2.0
- io.grpc.* version: 1.19.0 License: Apache 2.0 
