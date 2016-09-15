//-----------------------------------------------------------------------------
// Copyright (c) 2012-2014 Fusion-io, Inc.
// Copyright (c) 2014-2015 SanDisk Corp. or its affiliates.
// Copyright (c) 2015-2016 Western Digital Corporation or its affiliates.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
// * Redistributions of source code must retain the above copyright notice,
//   this list of conditions and the following disclaimer.
// * Redistributions in binary form must reproduce the above copyright notice,
//   this list of conditions and the following disclaimer in the documentation
//   and/or other materials provided with the distribution.
// * Neither the name of the Western Digital Corp. nor the names of its contributors
//   may be used to endorse or promote products derived from this software
//   without specific prior written permission.
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
// THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED.
// IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY,
// OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
// PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
// OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
// WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//-----------------------------------------------------------------------------

#include <errno.h>
#include <getopt.h>
#include <stdio.h>
#include <libgen.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <list>
#include <vector>
#include <fcntl.h>
#include <fstream>
#include <sstream>
#include <sys/stat.h>
#include <sys/time.h>
#include <signal.h>

#include "persistence.h"

#define DEFAULT_SEGMENTS 8
#define MAX_SEGMENTS 1024
#define DEFAULT_MAX_EXTENT 256
#define LARGEST_MAX_EXTENT 8192 // The VSL will subdivide into chunks of 2048 packets
#define DEFAULT_MIN_EXTENT 1
#define DEFAULT_ATOMIC_MIN_VECTORS 1
#define DEFAULT_CAPACITY_RATIO 1.0
#define DEFAULT_RUNTIME 0
#if defined(__linux__) || defined(__FreeBSD__) || defined(USERSPACE_HOST_OS_linux2) || defined(USERSPACE_HOST_OS_freebsd)
#define O_DIRECT_SUPPORTED true
#endif

#define VERSION_STRING "23735b22acb8 - 1.0"

using namespace std;

volatile bool g_interrupted = false;

void *interrupt(void* arg)
{
    time_t end_time = time(NULL) + *(uint64_t*)arg;
    while (time(NULL) < end_time)
    {
        usleep(250000);
        if (g_interrupted)
        {
            return 0;
        }
    }
    g_interrupted = true;
    return 0;
}

bool file_exists(string path)
{
    ifstream ifile(path.c_str());
    return ifile.good();
}

static bool parse_extents_switch(vector<uint64_t>* extents_vec, string extents, uint32_t num_segments)
{
    istringstream iss(extents);
    do
    {
        string sub;
        iss>>sub;
        if (!sub.length())
        {
            continue;
        }
        if (sub == "N" || sub == "n")
        {
            extents_vec->push_back(WRITE_INDEFINITELY);
        }
        else if (sub == "P" || sub == "p")
        {
            extents_vec->push_back(COMPLETE_PASS);
        }
        else
        {
            extents_vec->push_back(strtoul(sub.c_str(), 0, 0));
        }
    }
    while (iss);

    if (extents_vec->size() == 1)
    {
        for(uint32_t i = 1 ; i < num_segments ; i++)
        {
            extents_vec->push_back(extents_vec->at(0));
        }
    }
    else if (extents_vec->size() != num_segments)
    {
        cerr<<"Invalid input provided to --extents (-e) switch."<<endl;
        return false;
    }
    return true;
}

static bool parse_ioengine_switch(vector<ioengine_t>* ioengine_vec, string ioengine, uint32_t num_segments, uint32_t max_extent_len, uint32_t sector_size)
{
    istringstream iss(ioengine);
    bool atomics_in_play = false;
    do
    {
        string sub;
        iss>>sub;
        if (!sub.length())
        {
            continue;
        }
        if (sub == "psync" || sub == "PSYNC")
        {
            ioengine_vec->push_back(PSYNC);
        }
        else if (sub == "atomic" || sub == "ATOMIC")
        {
            ioengine_vec->push_back(ATOMIC);
            atomics_in_play = true;
        }
        else if (sub == "libaio" || sub == "LIBAIO")
        {
#ifndef LIBAIO_SUPPORTED
            throw runtime_error("LIBAIO is not supported with this build.");
#endif
            ioengine_vec->push_back(LIBAIO);

        }
        else
        {
            cerr<<"'"<<sub<<"' not understood by --ioengine switch."<<endl;
            return false;
        }
    }
    while (iss);

    if (ioengine_vec->size() == 1)
    {
        for(uint32_t i = 1 ; i < num_segments ; i++)
        {
            ioengine_vec->push_back(ioengine_vec->at(0));
        }
    }
    else if (ioengine_vec->size() != num_segments)
    {
        cerr<<"Invalid input provided to --ioengine (-i) switch."<<endl;
        return false;
    }

    if (atomics_in_play && (max_extent_len * sector_size) > IO_VECTOR_MAX_SIZE)
    {
        cerr<<"The maximum extent is too large for use with atomic writes."<<endl;
        cerr<<"For this blocksize please set no greater than "<<IO_VECTOR_MAX_SIZE / sector_size<<"."<<endl;
        return false;
    }

    return true;
}

static void usage(FILE *stream, const char *name)
{
    fprintf(stream, "Western Digital %s utility (%s)\n", name, VERSION_STRING);
    fprintf(stream, "  Continuously writes a block device in a given LBA range providing full\n");
    fprintf(stream, "  verification of all blocks (populated or not) across multiple write sessions.\n");
    fprintf(stream, "  Capacity Ratio is sustained once device is full & requires TRIM support if < 1.0.\n");
    fprintf(stream, "  Memory usage while verifying all block devices, or writing against a sparse block\n");
    fprintf(stream, "  device is ~250 MB per 2 million sectors (~1 TB of 512 byte formatted LBA space).\n");
    fprintf(stream, "  Note: To achieve a short verification time, use a low range-ratio.\n\n");
    fprintf(stream, "usage: %s [options] <block device>\n", name);
    fprintf(stream, "  Options:\n");
    fprintf(stream, "   -h, --help               Print help message.\n");
    fprintf(stream, "   -b, --blocksize          Sector size.  The sector size is automatically detected;\n");
    fprintf(stream, "                            however, it may be overridden with this parameter.\n");
    fprintf(stream, "                            If the auto-detection fails, the default block size is %u\n", DEFAULT_SECTOR_SIZE);
    fprintf(stream, "   -c, --sector-count       Number of sectors the actual device holds.\n");
    fprintf(stream, "                            Provide only if sparsely formatted.\n");
    fprintf(stream, "   -C, --capacity-ratio     Portion of capacity to be populated at any time.\n");
    fprintf(stream, "                            In true sparse cases, this is a portion of phyiscal\n");
    fprintf(stream, "                            capacity, not range.  Default is %0.1f, max is 1.0.\n", DEFAULT_CAPACITY_RATIO);
    fprintf(stream, "   -d, --atomic-min-vectors Smallest number of vectors per call.  Default is %u.\n", DEFAULT_ATOMIC_MIN_VECTORS);
    fprintf(stream, "   -D, --atomic-max-vectors Largest number of vectors per call.  Default is %u.\n", IO_VECTOR_LIMIT);
    fprintf(stream, "   -e, --extents            Extents to write or trim per segment.  If a single value is \n");
    fprintf(stream, "                            given, it will be applied to all segments.  Otherwise a space\n");
    fprintf(stream, "                            separated list needs to be given, one value per segment.\n");
    fprintf(stream, "                            Wildcards: P means complete the pass, N means write indefinitely.\n");
    fprintf(stream, "                            -e '0 0 0 0 0 P h 25 N' would write nothing to segments one\n");
    fprintf(stream, "                            through five, segment 6 would just have its pass completed, 25\n");
    fprintf(stream, "                            extents to segment seven and write forever on segment eight.\n");
    fprintf(stream, "                            Note, if using atomics, slightly fewer extents may be executed\n");
    fprintf(stream, "                            than specified because they did not fill out the group of vectors\n");
    fprintf(stream, "                            in the system call.\n");
    fprintf(stream, "   -f, --state-file         Path to the state file.\n");
    fprintf(stream, "   -F, --first-sector       A sector number to begin dividing each segment's LBA range from.\n");
    fprintf(stream, "                            If not provided, a random sector will be chosen.  Only meaningful\n");
    fprintf(stream, "                            if the --range-ratio < 1.0.\n");
    fprintf(stream, "   -i, --ioengine           Defines method in which I/O is submitted on each segment.  If a\n");
    fprintf(stream, "                            single value is given, it will be applied to all segments.\n");
    fprintf(stream, "                            Otherwise a space separated list needs to be given, one value per\n");
    fprintf(stream, "                            segment.  Supported ioengines are psync (default)");
#ifdef LIBAIO_SUPPORTED
    fprintf(stream, ", atomic and libaio.\n");
    fprintf(stream, "   -I, --iodepth            Number of I/Os each segment will keep in flight.  Requires the\n");
    fprintf(stream, "                            aio ioengine.  Default is 1.\n");
    fprintf(stream, "   -J, --iodepth-batch      Number of I/Os to submit at once.  Default is iodepth.\n");
    fprintf(stream, "   -K, --iodepth-batch-comp Number of I/Os to retrieve at once.  Default is 1.\n");
#else
    fprintf(stream, " and atomic.\n");
#endif
    fprintf(stream, "   -m, --minimum-extent     Minimum extent size in sectors.\n");
    fprintf(stream, "                            Note: The block device's last extent may be smaller.\n");
    fprintf(stream, "                            Default: %u\n", DEFAULT_MIN_EXTENT);
    fprintf(stream, "   -M, --maximum-extent     Maximum extent size in sectors.\n");
    fprintf(stream, "                            Default: %u\n", DEFAULT_MAX_EXTENT);
    fprintf(stream, "   -P, --print-operations   Prints all write operations recorded by statefile to stdout.\n");
    fprintf(stream, "                            {seg_num}[:passes_from_end]  Defaults to last 1 or 2 passes.\n");
    fprintf(stream, "   -p, --no-pers-trim       Do not enforce persistent trim during verification.\n");
    fprintf(stream, "   -r, --runtime            Seconds to run, 0 is forever.  Default is %u\n", DEFAULT_RUNTIME);
    fprintf(stream, "   -R, --range-ratio        Address range represented as a multiple of the\n");
    fprintf(stream, "                            sector count.  Default is %0.1f for normal formats\n", DEFAULT_RANGE_RATIO);
    fprintf(stream, "                            %0.1f for sparse formats.  Values over 3.0 not\n", DEFAULT_RANGE_RATIO_SPARSE);
    fprintf(stream, "                            recommended due to excessive memory usage.\n");
    fprintf(stream, "   -s, --seed               Integer value to seed the session.\n");
    fprintf(stream, "   -S, --segments           Specify segments in the range.  Each will be\n");
    fprintf(stream, "                            operated upon by a separate thread.  Default %u\n", DEFAULT_SEGMENTS);
#ifdef O_DIRECT_SUPPORTED
    fprintf(stream, "   -t, --use-o-direct       Use O_DIRECT.  Default for OSs that support it.\n");
    fprintf(stream, "   -u, --use-page-cache     Use normal cached I/O.  Default for OSs with no\n");
    fprintf(stream, "                            O_DIRECT support.  Implies --no-pers-trim.\n");
    fprintf(stream, "   -U, --skip-unwritten     Whether to skip verification of sectors never written.\n");
    fprintf(stream, "                            Provides faster verification on write runs that did not\n");
    fprintf(stream, "                            fill a complete pass.\n");
#endif
    fprintf(stream, "   -v, --version            Print version information.\n");
    fprintf(stream, "   -V, --verify [seg_num]   Verify data on the block device.  If no segment\n");
    fprintf(stream, "                            number is defined, all will be verified.\n");
}

static const char short_opts[] = "ha::b:c:C:d:D:e:f:F:i:I:J:K:m:M:P:pr:R:s:S:tuUvV::";
static struct option long_opts[] =
{
    {"help", no_argument, NULL, 'h'},
    {"blocksize", required_argument, NULL, 'b'},
    {"sector-count", required_argument, NULL, 'c'},
    {"capacity-ratio", required_argument, NULL, 'C'},
    {"atomic-min-vectors", required_argument, NULL, 'd'},
    {"atomic-max-vectors", required_argument, NULL, 'D'},
    {"extents", required_argument, NULL, 'e'},
    {"state-file", required_argument, NULL, 'f'},
    {"first-sector", required_argument, NULL, 'F'},
    {"ioengine", required_argument, NULL, 'i'},
#ifdef LIBAIO_SUPPORTED
    {"iodepth", required_argument, NULL, 'I'},
    {"iodepth-batch", required_argument, NULL, 'J'},
    {"iodepth-batch-comp", required_argument, NULL, 'K'},
#endif
    {"minimum-extent", required_argument, NULL, 'm'},
    {"maximum-extent", required_argument, NULL, 'M'},
    {"print-operations", required_argument, NULL, 'P'},
    {"no-pers-trim", no_argument, NULL, 'p'},
    {"runtime", required_argument, NULL, 'r'},
    {"range-ratio", required_argument, NULL, 'R'},
    {"seed", required_argument, NULL, 's'},
    {"segments", required_argument, NULL, 'S'},
#ifdef O_DIRECT_SUPPORTED
    {"use-o-direct", no_argument, NULL, 't'},
    {"use-page-cache", no_argument, NULL, 'u'},
#endif
    {"skip-unwritten", no_argument, NULL, 'U'},
    {"version", no_argument, NULL, 'v'},
    {"verify", optional_argument, NULL, 'V'},
    {0, 0, 0, 0},
};

static void on_sigint(int signum)
{
    g_interrupted = true;
}

//@brief Returns the long option given the short value.
string short_to_long_op(char c)
{
    for (int i = 0 ; long_opts[i].val != 0; i++)
    {
        if (long_opts[i].val == c)
        {
            return long_opts[i].name;
        }
    }
    return "";
}

static int print_operations(string statefile_path, string bt_param)
{
    Session* session = NULL;
    uint32_t segment = ~0;
    uint32_t num_passes = 0;

    if (!file_exists(statefile_path))
    {
        cerr<<"A valid statefile must be provided. '"<<statefile_path<<"' is not valid."<<endl;
        return ENOENT;
    }

    size_t pos = bt_param.find(":");
    if (pos == string::npos)
    {
        segment = strtoul(bt_param.c_str(), 0, 0);
    }
    else
    {
        stringstream(bt_param.substr(0, pos)) >> segment;
        if (bt_param.length() > pos + 1)
        {
            stringstream(bt_param.substr(pos + 1, string::npos)) >> num_passes;
        }
    }

    ifstream infile(statefile_path.c_str());
    Json::Value jroot;
    Json::Reader reader;
    if (!reader.parse(infile, jroot, false))
    {
        throw runtime_error(reader.getFormattedErrorMessages());
    }
    infile.close();
    session = new Session(-1, statefile_path, jroot["session"]);

    try
    {
        session->print_seg_operations(segment, num_passes);
    }
    catch (const exception &exc)
    {
        cerr<<exc.what()<<endl;
        delete session;
        return EINVAL;
    }
    delete session;
    return 0;
}


int main(int argc, char *argv[])
{
    const char *prog = basename(argv[0]);
    int opt;
    uint32_t atomic_min_vectors = DEFAULT_ATOMIC_MIN_VECTORS;
    uint32_t atomic_max_vectors = IO_VECTOR_LIMIT;
    double range_ratio = 0;
    double capacity_ratio = DEFAULT_CAPACITY_RATIO;
    bool persistent_trim_supported = false;
    bool use_o_direct = false;
    bool o_direct_specified = false;
#ifdef O_DIRECT_SUPPORTED
    persistent_trim_supported = true;
    use_o_direct = true;
#endif
    int64_t fd = -1;
    bool perform_verify = false;
    bool skip_unwritten_sectors = false;
    int verify_seg_num = -1;
    uint64_t seed = time(NULL);
    uint32_t segments = DEFAULT_SEGMENTS;
    uint64_t runtime = DEFAULT_RUNTIME;
    string extents = "N";
    uint32_t sector_size = DEFAULT_SECTOR_SIZE;
    uint64_t first_sector = NO_FIRST_SECTOR_DEFINED;
    string print_op_param = "NONE";
    uint32_t min_extent_len = DEFAULT_MIN_EXTENT;
    uint32_t max_extent_len = DEFAULT_MAX_EXTENT;
    string ioengine = "psync";
    uint32_t iodepth = 1;
    long iodepth_batch = ~0;
    long iodepth_batch_complete = 1;
    uint64_t specified_sector_count = 0;
    string statefile_path = "";
    int rc = 0;
    bool write_time_switch_passed = false;
    list<char> write_time_config_switches_passed;
    bool verify_time_switch_passed = false;
    bool resuming = false;
    bool forced_sector_size = false;

    while ((opt = getopt_long(argc, argv, short_opts, long_opts, NULL)) != -1)
    {
        switch (opt)
        {
        case 'v':
            fprintf(stdout, "%s\n", VERSION_STRING);
            return 0;
        case 'h':
            usage(stdout, prog);
            return 0;
        case 'd':
            atomic_min_vectors = strtoul(optarg, 0, 0);
            if (atomic_min_vectors < 1 || atomic_min_vectors > IO_VECTOR_LIMIT)
            {
                cerr<<"Minimum atomic vectors must be between 1 and "<<IO_VECTOR_LIMIT<<endl;
                return EINVAL;
            }
            write_time_config_switches_passed.push_back('d');
            break;
        case 'D':
            atomic_max_vectors = strtoul(optarg, 0, 0);
            if (atomic_max_vectors < 1 || atomic_max_vectors > IO_VECTOR_LIMIT)
            {
                cerr<<"Maximum atomic vectors must be between 1 and "<<IO_VECTOR_LIMIT<<endl;
                return EINVAL;
            }
            write_time_config_switches_passed.push_back('D');
            break;
#ifdef LIBAIO_SUPPORTED
        case 'I':
            iodepth = strtoul(optarg, 0, 0);
            if (iodepth > MAX_IO_DEPTH)
            {
                cerr<<"Maximum iodepth is "<<MAX_IO_DEPTH<<"."<<endl;
                return EINVAL;
            }
            write_time_config_switches_passed.push_back('I');
            break;
        case 'J':
            iodepth_batch = strtoul(optarg, 0, 0);
            write_time_config_switches_passed.push_back('J');
            break;
        case 'K':
            iodepth_batch_complete = strtoul(optarg, 0, 0);
            write_time_config_switches_passed.push_back('K');
            break;
#endif
        case 'i':
            ioengine = string(optarg);
            write_time_config_switches_passed.push_back('i');
            break;
        case 'm':
            min_extent_len = strtoul(optarg, 0, 0);
            write_time_config_switches_passed.push_back('m');
            break;
        case 'M':
            if (strtoul(optarg, 0, 0) > LARGEST_MAX_EXTENT)
            {
                cerr<<"Largest max extent allowed is "<<LARGEST_MAX_EXTENT<<"."<<endl;
                return EINVAL;
            }
            max_extent_len = strtoul(optarg, 0, 0);
            write_time_config_switches_passed.push_back('M');
            break;
        case 'b':
            sector_size = strtoul(optarg, 0, 0);
            write_time_config_switches_passed.push_back('b');
            forced_sector_size = true;
            break;
        case 'c':
            specified_sector_count = strtoul(optarg, 0, 0);
            write_time_config_switches_passed.push_back('c');
            break;
        case 'P':
            print_op_param = string(optarg);
            break;
        case 'p':
            persistent_trim_supported = false;
            write_time_config_switches_passed.push_back('p');
            break;
        case 'r':
            runtime = strtoul(optarg, 0, 0);
            write_time_switch_passed = true;
            break;
        case 'e':
            extents = string(optarg);
            write_time_switch_passed = true;
            break;
        case 'R':
            range_ratio = strtof(optarg, 0);
            write_time_config_switches_passed.push_back('R');
            break;
        case 'C':
            capacity_ratio = strtof(optarg, 0);
            write_time_config_switches_passed.push_back('C');
            break;
        case 's':
            seed = strtoull(optarg, 0, 0);
            write_time_config_switches_passed.push_back('s');
            break;
        case 'S':
            segments = strtoul(optarg, 0, 0);
            write_time_config_switches_passed.push_back('S');
            if (segments > MAX_SEGMENTS || segments < 1)
            {
                cerr<<"Segments must be between 0 and "<<MAX_SEGMENTS<<"."<<endl;
                return EINVAL;
            }
            break;
        case 'f':
            statefile_path = string(optarg);
            break;
        case 'F':
            first_sector = strtoul(optarg, 0, 0);
            write_time_config_switches_passed.push_back('F');
            break;
        case 't':
            use_o_direct = true;
            o_direct_specified = true;
            break;
        case 'u':
            use_o_direct = false;
            o_direct_specified = true;
            persistent_trim_supported = false;
            break;
        case 'U':
            skip_unwritten_sectors = true;
            verify_time_switch_passed = true;
            break;
        case 'V':
            perform_verify = true;
            if (optarg)
            {
                verify_seg_num = strtoul(optarg, 0, 0);
            }
            break;
        default:
            return EINVAL;
        }
    }
    assert(MAX_IO_DEPTH >= IO_VECTOR_LIMIT);

    // Print operations, then exit.
    if (print_op_param.compare("NONE") != 0)
    {
        return print_operations(statefile_path, print_op_param);
    }

    // We're going to operate on a block device
    if (argc - optind != 1)
    {
        if (optind >= argc)
        {
            fprintf(stderr, "Missing block device argument\n");
        }
        else
        {
            fprintf(stderr, "Too many arguments.\n");
        }
        return EINVAL;
    }

    if (!perform_verify && verify_time_switch_passed)
    {
        cerr<<"WARNING! You have specified switches that apply only to ";
        cerr<<"verification, but you are not performing verification."<<endl;
    }

    if (perform_verify && (write_time_switch_passed || !write_time_config_switches_passed.empty()))
    {
        cerr<<"WARNING! The following switches were ignored because they only pertain to ";
        cerr<<"invocations that write: ";
        for (list<char>::iterator it = write_time_config_switches_passed.begin();
                it != write_time_config_switches_passed.end(); it++)
        {
            cerr<<short_to_long_op(*it)<<", ";
        }
        cerr<<endl;
    }

#ifdef LIBAIO_SUPPORTED
    if (iodepth_batch == ~0)
    {
        iodepth_batch = iodepth;
    }

    if (iodepth > MAX_IO_DEPTH)
    {
        cerr<<"Maximum iodepth is "<<MAX_IO_DEPTH<<endl;
        return EINVAL;
    }

    if (iodepth_batch > (long)iodepth)
    {
        cerr<<"iodepth-batch cannot be greater than the iodepth."<<endl;
        return EINVAL;
    }

    if (iodepth_batch_complete > (long)iodepth)
    {
        cerr<<"iodepth-batch-complete cannot be greater than the iodepth."<<endl;
        return EINVAL;
    }
#endif

    if (atomic_max_vectors < atomic_min_vectors)
    {
        cerr<<"Maximum atomic vector is smaller than the min atomic vector."<<endl;
        return EINVAL;
    }

    if (statefile_path.length() == 0)
    {
        ostringstream o;
        o<<"statefile_"<<seed<<"_"<<basename(argv[optind]);
        statefile_path = o.str();

        if (file_exists(statefile_path))
        {
            cerr<<statefile_path<<" already exists. If you want to resume ";
            cerr<<"writing, pass this statefile in with the --state-file switch."<<endl;
            return EEXIST;
        }
    }

    if (perform_verify && !file_exists(statefile_path))
    {
        cout<<"Statefiles are required for verification."<<endl;
        return EINVAL;
    }

    if ((range_ratio >= 1.0 || range_ratio == 0) && first_sector != NO_FIRST_SECTOR_DEFINED)
    {
        cerr<<"Specifying a first sector is not meaningful unless range ratio < 1.0."<<endl;
        return EINVAL;
    }

    // Register for signals to exit gracefully if interrupted.
    struct sigaction act;
    act.sa_handler = on_sigint;
    sigemptyset(&act.sa_mask);
    act.sa_flags = SA_RESTART;
    sigaction(SIGHUP,  &act, NULL);
    sigaction(SIGINT,  &act, NULL);

    Session* session = NULL;
    struct timeval elapsed_time = {0, 0};

    try
    {
        Json::Value jroot;

        if (file_exists(statefile_path))
        {
            resuming = true;
            ifstream infile(statefile_path.c_str());
            Json::Reader reader;
            if (!reader.parse(infile, jroot, false))
            {
                throw runtime_error(reader.getFormattedErrorMessages());
            }
            infile.close();
            // Allow users to override statefile setting.
            if (!o_direct_specified)
            {
                use_o_direct = jroot["session"]["use_o_direct"].asBool();
            }
        }
        int flags = O_RDWR;

#ifdef O_DIRECT_SUPPORTED
        if (use_o_direct)
        {
            flags |= O_DIRECT;
        }
#endif // O_DIRECT_SUPPORTED
        mode_t mode = S_IRUSR | S_IWUSR;
        fd = open(argv[optind], flags, mode);

        if (fd < 0)
        {
            cerr<<"Error occurred while opening block device.\n";
            rc = EBADF;
            goto exit;
        }

        // Get the block size if the user has not specified it on the commandline.
        if (forced_sector_size == false)
        {
            struct stat sb;
            if (stat(argv[optind], &sb) == 0)
            {
                sector_size = sb.st_blksize;
            }
        }

        if (resuming)
        {
            session = new Session(fd, statefile_path, jroot["session"]);
        }
        else
        {
            vector<ioengine_t> ioengine_vec;
            if (!parse_ioengine_switch(&ioengine_vec, ioengine, segments, max_extent_len,
                                       sector_size))
            {
                rc = EINVAL;
                goto exit;
            }
            session = new Session(seed, segments, fd, &ioengine_vec, atomic_min_vectors,
                                  atomic_max_vectors, min_extent_len, max_extent_len,
                                  specified_sector_count, sector_size, persistent_trim_supported,
                                  use_o_direct, range_ratio, capacity_ratio, first_sector,
                                  iodepth, iodepth_batch, iodepth_batch_complete, statefile_path);
        }
    }
    catch (const runtime_error & e)
    {
        cerr<<e.what()<<endl;
        rc = EINVAL;
        goto exit;
    }

    if (resuming && !write_time_config_switches_passed.empty())
    {
        cerr<<"WARNING! The following switches: ";
        for (list<char>::iterator it = write_time_config_switches_passed.begin();
                it != write_time_config_switches_passed.end(); it++)
        {
            cerr<<short_to_long_op(*it)<<", ";
        }
        cerr<<"will be ignored because all write time settings are already set in the provided statefile."<<endl;
        cerr<<"If you want to start a new session with different settings, reset your ";
        cerr<<"device's state and do not reference an existing statefile."<<endl;
    }

    if (perform_verify)
    {
        rc = session->verify(verify_seg_num, skip_unwritten_sectors, &elapsed_time, false);

        if (skip_unwritten_sectors)
        {
            cout<<"Verification of never written sectors skipped."<<endl;
        }

        if (rc == NO_FAULTS_FOUND)
        {
            if (session->corruptions_found())
            {
                rc |= FAULTS_FOUND_PREVIOUSLY;
                cout<<"No corruptions found in this verification, but the statefile indicates ";
                cout<<"faults have been encountered in previously performed verifications.  Failing."<<endl;
            }
            else
            {
                cout<<"No corruptions found in this or previous verifications."<<endl;;
            }
        }
        else
        {
            if (rc & IO_ERROR)
            {
                cout<<"One or more IO errors occurred. ";
            }

            if (rc & INTERRUPTED)
            {
                cout<<"Verification was interrupted. ";
            }

            if (rc & FAULTS_FOUND)
            {
                cout<<"Faults found.";
            }
            cout<<endl;
        }
        cout<<elapsed_time.tv_sec<<"s "<<elapsed_time.tv_usec<<"us elapsed.";
        if (session->is_pers_trim_supported())
        {
            cout<<" Trims were checked to ensure they are all zeros."<<endl;
        }
        else
        {
            cout<<" Trims were not checked to ensure they are all zeros."<<endl;
        }
    }
    else
    {
        // Verify any unacked extents to verify we can safely resume.
        rc = session->verify(verify_seg_num, skip_unwritten_sectors, &elapsed_time, true);

        if (rc != NO_FAULTS_FOUND)
        {
            if (rc & FAULTS_FOUND)
            {
                cout<<"Faults found while examining interruption point, cannot continue."<<endl;
            }
            if (rc & INTERRUPTED)
            {
                cout<<"Interrupted while examining interruption point.  No data written."<<endl;
            }
            if (rc & IO_ERROR)
            {
                cout<<"I/O errors detected. Stopping now!"<<endl;
            }
            goto exit;
        }

        if (resuming)
        {
            cout<<"Resuming ";
        }
        else
        {
            cout<<"Starting ";
        }
        cout<<session->get_num_segments()<<" segment (thread) ";
        if (use_o_direct)
        {
            cout<<"O_DIRECT ";
        }
        else
        {
            cout<<"CACHED ";
        }
        cout<<"write session.  State file: "<<statefile_path<<endl;

        pthread_t timer;

        vector<uint64_t> extents_vec;
        if (!parse_extents_switch(&extents_vec, extents, session->get_num_segments()))
        {
            usage(stderr, prog);
            rc = EINVAL;
            goto exit;
        }

        // Start the runtime ticker
        if (runtime)
        {
            pthread_create(&timer, NULL, &interrupt, &runtime);
        }

        uint64_t orig_extents_written = session->get_extents_written();
        uint64_t orig_extents_trimmed = session->get_extents_trimmed();
        uint64_t orig_sectors_written = session->get_sectors_written();
        uint64_t orig_sectors_trimmed = session->get_sectors_trimmed();

        try
        {
            rc = session->random_execute(extents_vec, &elapsed_time);
        }
        catch (const runtime_error & e)
        {
            cerr<<e.what()<<endl;
            rc = EINVAL;
            goto exit;
        }
        double elapsed_seconds = static_cast<double>(elapsed_time.tv_sec);
        elapsed_seconds += static_cast<double>(elapsed_time.tv_usec) / 1000000.0;

        uint64_t sectors_written = session->get_sectors_written() - orig_sectors_written;
        uint64_t sectors_trimmed = session->get_sectors_trimmed() - orig_sectors_trimmed;

        cout<<"Working seconds: "<<elapsed_seconds<<endl;
        cout<<"Sectors written: "<<sectors_written;
        cout<<" this invocation ("<<session->get_sectors_written()<<" to date)."<<endl;
        cout<<"Sectors trimmed: "<<sectors_trimmed;
        cout<<" this invocation ("<<session->get_sectors_trimmed()<<" to date)."<<endl;
        cout<<"Extents written: "<<session->get_extents_written() - orig_extents_written;
        cout<<" this invocation ("<<session->get_extents_written()<<" to date)."<<endl;
        cout<<"Extents trimmed: "<<session->get_extents_trimmed() - orig_extents_trimmed;
        cout<<" this invocation ("<<session->get_extents_trimmed()<<" to date)."<<endl;

        uint64_t total_bytes_touched = (sectors_written + sectors_trimmed) * session->sector_size_;
        if (sectors_trimmed > 0)
        {
            cout<<"Write and Trim throughput: ";
        }
        else
        {
            cout<<"Write throughput: ";
        }
        cout<<(double(total_bytes_touched) / elapsed_seconds) / double((1<<20ULL))<<" MB/s"<<endl;

        g_interrupted = true;
        if (runtime)
        {
            void* tresult;
            pthread_join(timer, &tresult);
        }
    }
    goto exit;

exit:
    if (session)
    {
        delete session;
    }

    if (fd >= 0)
    {
        close(fd);
    }
    return rc;
}
