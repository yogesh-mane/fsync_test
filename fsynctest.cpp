#include <iostream>
#include <chrono>
#include <ostream>
#include <string>
#include <libgen.h>
#include <sstream>
#include <cerrno>
#include <cstring>
#include <string>
#include <system_error>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

namespace
{
    std::chrono::time_point<std::chrono::steady_clock> getElapsedTimeMonitorTimestamp()
    {
        return std::chrono::steady_clock::now();
    }

    template <decltype(getElapsedTimeMonitorTimestamp) getTimestamp = getElapsedTimeMonitorTimestamp>
    class ElapsedTimeMonitorImpl
    {
    public:
        ElapsedTimeMonitorImpl(const std::string& operation):
            operation(operation),
            start(getTimestamp())
        {
        }
        ~ElapsedTimeMonitorImpl()
        {
            auto elapsed(getTimestamp() - start);
            std::cout
                << "Operation \"" << operation << "\" took "
                << std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count()
                << "ms to complete." << std::endl;
        }

    private:
        const std::string operation;
        std::chrono::time_point<std::chrono::steady_clock> start;
    };

    // With C++17 this can be removed and template class itself can be named ElapsedTimeMonitor
    using ElapsedTimeMonitor = ElapsedTimeMonitorImpl<>;

    class CommittedFile
    {
    public:
        explicit CommittedFile(const std::string& filePath);

        virtual ~CommittedFile();

        virtual std::string read() const;

        virtual void write(const std::string& data);

        virtual std::string getPath() const;

    private:
        void cleanup();

        std::string filePath;
    };

    std::string buildCommittedFileError(const std::string& func,
                                        const std::string& directory,
                                        const std::string& file1,
                                        const std::string& file2,
                                        int error)
    {
        std::ostringstream os;
        os << func << "(\"" << directory;
        if (!file1.empty())
            os << '/' << file1;
        if (!file2.empty())
            os << "\", \"" << directory << '/' << file2;
        os << "\"): " << strerror(error);
        return os.str();
    }

    std::string buildCommittedFileReadError(const std::string& func,
                                            const std::string& file,
                                            int error)
    {
        std::ostringstream os;
        os << func << "(\"" << file << "\") ";
        os << strerror(error);
        return os.str();
    }

    class BaseFd
    {
    public:
        BaseFd(const std::string& directory,
               const std::string& file,
               int fd);

        virtual ~BaseFd();

        void sync();

        void close();

        operator int() const noexcept { return fd; }

        BaseFd(const BaseFd&) = delete;
        BaseFd(BaseFd&&) = delete;
        BaseFd& operator=(const BaseFd&) = delete;
        BaseFd& operator=(BaseFd&&) = delete;

        const std::string directory;
        const std::string file;
        int fd;
    };

    class DirFd: public BaseFd
    {
    public:
        DirFd(const std::string& directory);

        void unlink(const std::string& file);

        void renameFile(const std::string& oldFile, const std::string& newFile);

    private:
        static const std::string NO_FILE;
    };

    class WriteFd: public BaseFd
    {
    public:
        WriteFd(DirFd& dirFd, const std::string& file);

        void writeAll(const void* data, size_t size);
    };

    std::string dirName(const std::string& filePath)
    {
        char buffer[filePath.size() + 1];
        memcpy(buffer, filePath.c_str(), filePath.size() + 1);
        return dirname(buffer);
    }

    std::string baseName(const std::string& filePath)
    {
        char buffer[filePath.size() + 1];
        memcpy(buffer, filePath.c_str(), filePath.size() + 1);
        return basename(buffer);
    }

    std::string readFile(const std::string& filePath)
    {
        auto fd(open(filePath.c_str(), O_RDONLY | O_CLOEXEC));
        if (fd == -1)
            throw std::system_error(errno, std::system_category(), buildCommittedFileReadError("open", filePath, errno).c_str());

        std::ostringstream os;
        char buffer[4096] = {};
        ssize_t len = 0;
        while ((len = read(fd, &buffer, sizeof(buffer))) > 0)
            os << std::string(buffer, static_cast<size_t>(len));

        const int savedErrno(errno);
        close(fd);
        if (len < 0)
            throw std::system_error(savedErrno, std::system_category(), buildCommittedFileReadError("read", filePath, savedErrno).c_str());

        return os.str();
    }

    std::string getRandomData()
    {
        auto now(std::chrono::system_clock::now());
        auto nowTimeT(std::chrono::system_clock::to_time_t(now));
        return std::ctime(&nowTimeT);

    }
}

void usage()
{
    std::cout << "Usage: fsynctest <filename> <count>" << std::endl;
    exit(0);
}

void writeFile(const std::string& filename)
{
    ElapsedTimeMonitor dummy("Write file");
    CommittedFile cf(filename);
    cf.write(getRandomData());
}

int main(int argc, const char* argv[])
{
    if (argc != 3)
        usage();

    std::string filename = argv[1];
    long count(std::atoi(argv[2]));
    if (count < 1)
        usage();
    for(long i = 0; i < count; ++i)
        writeFile(filename);
}

BaseFd::BaseFd(const std::string& directory,
               const std::string& file,
               int fd):
    directory(directory),
    file(file),
    fd(fd)
{
}

BaseFd::~BaseFd()
{
    if (fd >= 0)
        /* Ignore errors */
        ::close(fd);
}

void BaseFd::sync()
{
    if (::fsync(fd) == -1)
        /**
         * @todo In theory ENOSPC and EDQUOT errors could be recovered
         * by retrying later. If there is such retry logic at upper
         * layer, then we should indicate that retrying is worth it in
         * the exception. Currently that sounds very unlikely.
         */
        throw std::system_error(errno, std::system_category(), buildCommittedFileError("fsync", directory, file, "", errno).c_str());
}

void BaseFd::close()
{
    if (fd >= 0)
    {
        const int copy(fd);
        fd = -1;
        if (::close(copy) == -1)
            throw std::system_error(errno, std::system_category(), buildCommittedFileError("close", directory, file, "", errno).c_str());
    }
}

const std::string DirFd::NO_FILE;

DirFd::DirFd(const std::string& directory):
    BaseFd(directory,
           NO_FILE,
           ::open(directory.c_str(), O_RDONLY | O_CLOEXEC))
{
    if (fd == -1)
        throw std::system_error(errno, std::system_category(), buildCommittedFileError("open", directory, "", "", errno).c_str());
}

void DirFd::unlink(const std::string& file)
{
    if ((::unlinkat(fd, file.c_str(), 0) == -1) && (errno != ENOENT))
        throw std::system_error(errno, std::system_category(), buildCommittedFileError("unlink", directory, file, "", errno).c_str());
}

void DirFd::renameFile(const std::string& oldFile, const std::string& newFile)
{
    if (::renameat(fd,
                 oldFile.c_str(),
                 fd,
                 newFile.c_str()) == -1)
        throw std::system_error(errno, std::system_category(), buildCommittedFileError("rename", directory, oldFile, newFile, errno).c_str());
}

WriteFd::WriteFd(DirFd& dirFd, const std::string& file):
    BaseFd(dirFd.directory,
           file,
           ::openat(dirFd,
                    file.c_str(),
                    O_CREAT | O_WRONLY | O_TRUNC | O_CLOEXEC,
                    S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH))
{
    if (fd == -1)
        throw std::system_error(errno, std::system_category(), buildCommittedFileError("open", directory, file, "", errno).c_str());
}

void WriteFd::writeAll(const void* data, size_t size)
{
    size_t written(0);
    while (written < size)
    {
        const ssize_t ret(::write(fd, static_cast<const char*>(data) + written, size - written));
        if (ret < 0)
            /**
             * @todo In theory ENOSPC and EDQUOT errors could be recovered
             * by retrying later. If there is such retry logic at upper
             * layer, then we should indicate that retrying is worth it in
             * the exception. Currently that sounds very unlikely.
             */
            throw std::system_error(errno, std::system_category(), buildCommittedFileError("write", directory, file, "", errno).c_str());
        else
            written += static_cast<size_t>(ret);
    }
}

CommittedFile::CommittedFile(const std::string& filePath):
    filePath(filePath)
{
    cleanup();
}

CommittedFile::~CommittedFile()
{
}

void CommittedFile::write(const std::string& data)
{
    DirFd dirFd(dirName(filePath));
    /*
     * First write and sync work-file. Do not touch real-file.
     */
    auto fileName(baseName(filePath));
    auto workFileName(fileName + ".work");
    WriteFd workFileFd(dirFd, workFileName);
    workFileFd.writeAll(data.data(), data.size());
    workFileFd.sync();
    workFileFd.close();
    /**
     * Posix guarantees that rename is atomic...
     */
    dirFd.renameFile(workFileName, fileName);
    /**
     * ... and with a directory fsync data is actually stored on disk
     * See: https://lwn.net/Articles/457667/
     */
    dirFd.sync();
    dirFd.close();
}

std::string CommittedFile::read() const
{
    return readFile(filePath);
}

void CommittedFile::cleanup()
{
    /**
     * Remove possibly existing old work file
     */
    const auto directory(dirName(filePath));
    DirFd dirFd(directory);
    const auto fileName(baseName(filePath));
    const auto workFileName(fileName + ".work");
    dirFd.unlink(workFileName);
    dirFd.close();
}

std::string CommittedFile::getPath() const
{
    return filePath;
}
