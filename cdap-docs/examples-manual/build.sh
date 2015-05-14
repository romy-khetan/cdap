#!/usr/bin/env bash

# Copyright © 2014-2015 Cask Data, Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
  
# Build script for guide docs
#
# Copies the original README from the mounted GitHub repo in the local filesystem
# running it through sed to modify all image links to be relative to the build/_includes directory.
# This modified README is included in the source RST files, and is used by Sphinx to build the HTML.

source ../_common/common-build.sh

CHECK_INCLUDES=${TRUE}
TUTORIAL_WISE="tutorial-wise"

function guide_rewrite_sed() {
  # Re-writes the links in the RST file to point to a local copy of any image links.
  # Looks for and downloads any image links
  echo "Re-writing using sed ${1} ${2}"
  local includes_dir=${1}
  local guide=${2}
  local project_version=${PROJECT_SHORT_VERSION}
  
  local source1="https://raw.githubusercontent.com/cdap-guides"
  if [ "x${GIT_BRANCH_TYPE:0:7}" == "xdevelop" ]; then
    local source2="develop"
  else
    local source2="release/cdap-${project_version}-compatible"
  fi
  local readme="README.rst"
  local readme_source="README_SOURCE.rst"

  local redirect="\.\./\.\./build/_includes" # Target, 2 redirects, escaped
  
  mkdir ${includes_dir}/${guide}
  curl --silent ${source1}/${guide}/${source2}/${readme} --output ${includes_dir}/${guide}/${readme_source}

  # Find and download any images
  local images=`grep -o ".. image:: .*" ${includes_dir}/${guide}/README_SOURCE.rst | cut -d ' ' -f 3`
  if [ "x${images}" != "x" ]; then
    for image in ${images}; do
      local image_file=`basename ${image}`
      curl --silent ${source1}/${guide}/${source2}/${image} --output ${includes_dir}/${guide}/${image_file}
    done
  fi
  
  # Rewrite image and code 
  sed -e "s|image:: docs/images/|image:: ${redirect}/${guide}/|g" -e "s|.. code:: |.. code-block:: |g" ${includes_dir}/${guide}/${readme_source} > ${includes_dir}/${guide}/${readme}
}

function download_file() {
  # Downloads a file to the includes directory, and checks that it hasn't changed.
  # Uses md5 hashes to monitor if any files have changed.
  local includes_dir=${1}
  local source_dir=${2}
  local file_name=${3}
  local md5_hash=${4}
  local target=${includes_dir}/${file_name}
  
  if [ ! -d "${includes_dir}" ]; then
    mkdir ${includes_dir}
    echo "Creating Includes Directory: ${includes_dir}"
  fi

  echo "Downloading using curl ${file_name} from ${source_dir}"
  curl ${source_dir}/${file_name} --output ${target} --silent
  test_an_include ${md5_hash} ${target}
}

function download_includes() { 
  echo_red_bold "Downloading source files includes from GitHub..."
  version
  
  local includes=${1}/${TUTORIAL_WISE}
  local project_version=${PROJECT_SHORT_VERSION}

  local source1="https://raw.githubusercontent.com/caskdata/cdap-apps"
  if [ "x${GIT_BRANCH_TYPE:0:7}" == "xdevelop" ]; then
    local source2="develop"
  else
    local source2="release/cdap-${project_version}-compatible"
  fi

  local project_source="${source1}/${source2}/Wise"
  local project_main=$project_source/src/main/java/co/cask/cdap/apps/wise
  local project_test=$project_source/src/test/java/co/cask/cdap/apps/wise
  local project_img=$project_source/docs/img

  # 1:Includes directory 2:GitHub directory 3:Java filename   4:MD5 hash of file
  download_file $includes $project_main BounceCountsMapReduce.java f2f8d36e4049ba69b40282057accf38a
  download_file $includes $project_main BounceCountStore.java      d476c15655c6a6c6cd7fe682dea4a8b7
  download_file $includes $project_main PageViewStore.java         576d76c60b8f5fddeee916a87318d209
  download_file $includes $project_main WiseApp.java               69825da7d5f6d1852fd5d28415418a45
  download_file $includes $project_test WiseAppTest.java           7256c18cb80f59b4a9abcb5da320b337
  download_file $includes $project_main WiseFlow.java              2deba0633a0dcca14ef426929f543872
  download_file $includes $project_main WiseWorkflow.java          8fe51eed165e85d95c4f5e25953e3489
  download_file $includes $project_main WiseService.java           dccfeb2d5726a031b5aff9897ccf8257

  echo_red_bold "Downloading image files from GitHub..."
  download_file $includes $project_img wise_architecture_diagram.png f01e52df149f10702d933d73935d9f29
  download_file $includes $project_img wise_explore_page.png         5136132e4e3232a216c12e2fe9d1b0c4
  download_file $includes $project_img wise_flow.png                 4a79853f2b5a0ac45929d0966f7cd7f5
  download_file $includes $project_img wise_store_page.png           15bcd8dac10ab5d1c643fff7bdecc52d

  echo_red_bold "Download file and any images and re-writes all the image links..."
  guide_rewrite_sed $1 cdap-bi-guide 
  guide_rewrite_sed $1 cdap-cube-guide 
  guide_rewrite_sed $1 cdap-etl-adapter-guide 
  guide_rewrite_sed $1 cdap-flow-guide
  guide_rewrite_sed $1 cdap-flume-guide
  guide_rewrite_sed $1 cdap-kafka-ingest-guide
  guide_rewrite_sed $1 cdap-mapreduce-guide
  guide_rewrite_sed $1 cdap-spark-guide
  guide_rewrite_sed $1 cdap-timeseries-guide
  guide_rewrite_sed $1 cdap-twitter-ingest-guide
}

run_command ${1}
