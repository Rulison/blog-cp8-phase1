BLOG submission for CP8 Phase 1

Tested on an AWS t2.small instance.  Minimum 2GiB memory.

./blog-setup.sh installs Java Python, and a version of Blog (in Java).

./run-submissions.sh [test files ...] translates each test file to Blog input, runs the Blog inference engine, then formats the output in the specified json format.

./setup-and-run.sh [test files ...] runs both of the above scripts.

[test files] are expected to be in the format userNN/whatever.

An example command is:
./setup-and-run.sh user35/USER_35.in.json user38/USER_38.in.json.