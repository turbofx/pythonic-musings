#!/usr/bin/python

# Death by infinite paper cuts!
# This program is designed to suss out a performance issue with GFS2.
# Thus I can have a benchmark to work against.
# Hypotheis:
# Creating lots and lots of small files on gfs2 causes it massive indigestion
# because of the distributed lock manager

import os, sys, datetime, argparse
from threading import Thread
from multiprocessing import Process
from time import sleep

RANDOM_DEVICE = '/dev/urandom'

DEBUG = False

def debug(msg):
    if DEBUG:
        print "DEBUG:", datetime.datetime.now(),':', msg

def error(msg):
    print "ERROR:", datetime.datetime.now(),':', msg


class pc_object:
    start_dir = ''
    write_count = 0
    filename_prefix = ''
    status = 0

class paper_cuts(object):
    def __init__(self, size_of_file, num_of_files, num_of_threads, dir_location, default_filename):
        self.size_of_file = size_of_file
        self.default_filename = default_filename
        self.dir_location = dir_location
        self.pcobj = {} # paper cut thread objects
        self.num_of_files = num_of_files
        self.num_of_threads = num_of_threads


    def generate_data(self):
        x = open(RANDOM_DEVICE, 'rb')
        result = x.read(self.size_of_file)
        x.close()
        return result
    

    def start_threads(self, thread_type=1, multi_proc=False):
        '''
        This function spins up thread_count number of threads
        plus any other threads needed to clean up the mess.
        the thread_type determines the type of threads
        1: All threads operate out of a single dirs
        2: All threads operate out of seperate dirs
        '''
        debug("Spinning up Worker engines!")
        for x in range(self.num_of_threads):
            self.pcobj[x] = pc_object()
            if thread_type == 1:
                self.pcobj[x].start_dir = self.dir_location
            elif thread_type == 2:
                self.pcobj[x].start_dir = self.dir_location + '/thread_%d' % x
            else: # maybe they picked something crazy?
                self.pcobj[x].start_dir = self.dir_location

            self.pcobj[x].filename_prefix = '%s_%d_' % (self.default_filename, x)
            self.pcobj[x].num_of_files = self.num_of_files

            if multi_proc:
                self.pcobj[x].worker_engine_thread = Process(target=self.worker_engine, args=(x,))
            else:
                self.pcobj[x].worker_engine_thread = Thread(target=self.worker_engine, args=(x,))

            self.pcobj[x].worker_engine_thread.start()
        return


    def worker_engine(self, tnum):
        '''
        This function runs the various functions.
        By abstracting it like this it's easy to write additional functions to do additional stuff.
        '''
        # create
        self.create_files(tnum)
        debug("[%d] Delete Phase!" % tnum)

        # validate
        self.validate_files(tnum)

        # delete
        self.delete_files(tnum)
        debug("[%d] Good Night!" % tnum)

        # Mark the thread as dead.
        self.pcobj[tnum].status = "DEAD"
        return


    def create_files(self, tnum):
        ''' 
        This function serially creates num_of_files
        and records how long it look.
        '''
        cwd = os.getcwd()
        if not os.path.isdir(self.pcobj[tnum].start_dir):
            os.mkdir(self.pcobj[tnum].start_dir)
        os.chdir(self.pcobj[tnum].start_dir)
        
        self.pcobj[tnum].status = "Generating data!"
        chunk = self.generate_data()
        self.pcobj[tnum].status = "Complete."
        
        debug("[%d] Creating Files" % tnum)
        self.pcobj[tnum].status = "Running"        
        self.pcobj[tnum].create_files_start_time = datetime.datetime.now()
        fname_prefix = self.pcobj[tnum].filename_prefix
        while (self.pcobj[tnum].write_count < self.num_of_files):
            x = os.open(fname_prefix + str(self.pcobj[tnum].write_count), os.O_CREAT|os.O_WRONLY)
            if os.write(x, chunk) != len(chunk):
                error("Error writing count: %d" % count)
            os.close(x)
            debug("[%d] created file: %s" % (tnum, (fname_prefix + str(self.pcobj[tnum].write_count))))
            self.pcobj[tnum].write_count += 1
        
        self.pcobj[tnum].create_files_stop_time = datetime.datetime.now()
        os.chdir(cwd)
        self.pcobj[tnum].status = "CREATE_DONE"
        self.pcobj[tnum].create_files_running_time = (self.pcobj[tnum].create_files_stop_time - 
                                                      self.pcobj[tnum].create_files_start_time)
        return

        
    def validate_files(self, tnum):
        '''
        This funcion validates that the files that create files
        wrote actually exist on the filesystem
        '''
        cwd = os.getcwd()
        os.chdir(self.pcobj[tnum].start_dir)
        count = 0
        check_count = 0
        try_count = 5
        self.pcobj[tnum].validate_files_start_time = datetime.datetime.now()
        done = False
        while not done:
            while (count < self.pcobj[tnum].write_count):
                if os.path.isfile(self.pcobj[tnum].filename_prefix + str(count)):
                    check_count += 1
                count += 1

            if check_count == self.pcobj[tnum].write_count:
                done = True
            else:
                check_count = 0
                count = 0
                try_count -= 1

            if try_count == 0: # We'll try 6 times and then give up
                done = True
        self.pcobj[tnum].validate_files_stop_time = datetime.datetime.now()
        self.pcobj[tnum].validate_files_running_time = (self.pcobj[tnum].validate_files_stop_time -
                                                        self.pcobj[tnum].validate_files_start_time)
        os.chdir(cwd)
        return
        
                

    def delete_files(self, tnum):
        '''
        This function deletes files...
        '''
        cwd = os.getcwd()
        os.chdir(self.pcobj[tnum].start_dir)
        count = 0
        self.pcobj[tnum].delete_files_start_time = datetime.datetime.now()
        debug("[%d] files to delete: %d" % (tnum, self.pcobj[tnum].write_count))
        #os.system('rm -f %s' % (self.pcobj[tnum].filename_prefix + '*'))
        while (count <= self.pcobj[tnum].write_count):
            try:
                if os.path.isfile('./' + self.pcobj[tnum].filename_prefix + str(count)):
                    debug("[%d] Deleting file: %s" % (tnum, (self.pcobj[tnum].filename_prefix + str(count))))
                    os.unlink('./' + self.pcobj[tnum].filename_prefix + str(count))
                else:
                    debug("[%d] Isn't a file? '%s'" % (tnum, (self.pcobj[tnum].filename_prefix + str(count))))
            except OSError:
                error("[%d] Unable to delete file: '%s'" % (tnum, (self.pcobj[tnum].filename_prefix + str(count))))
            count += 1
        
        self.pcobj[tnum].delete_files_stop_time = datetime.datetime.now()
        self.pcobj[tnum].status = "DELETE_DONE"
        os.chdir(cwd)
        self.pcobj[tnum].delete_files_running_time = (self.pcobj[tnum].delete_files_stop_time - 
                                                      self.pcobj[tnum].delete_files_start_time)
        return


    def thread_cleanup(self):
        done = False
        while not done:
            count = 0
            for k in self.pcobj.keys():
                if self.pcobj[k].status == "DEAD":
                    count += 1
            if count == self.num_of_threads:
                debug("All threads reporting dead... leaving!")
                done = True
            else:
                sleep(1) # sleep on second and try again.
        return

    
    def print_results(self):
        '''
        This function takes all the data in the pcobj dictionary and creates a report
        '''
        print "THR : Action : Time Taken"
        print "----|--------|-----------"
        for k in self.pcobj.keys():
            print "%03d : Create : %s" % (k, self.pcobj[k].create_files_running_time)
            print "%03d : Delete : %s" % (k, self.pcobj[k].delete_files_running_time)
            

def process_commandline():
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--blocksize', default='1024')
    parser.add_argument('-l', '--location')
    parser.add_argument('-n', '--num-of-files', default='1000')
    parser.add_argument('-t', '--num-of-threads', default='10')
    parser.add_argument('-f', '--file-prefix', default='dbipc')
    parser.add_argument('-p', '--private', default='1')
    parser.add_argument('-m', '--multiprocessing', action='store_true') 
    args = parser.parse_args()
    if args.location == None:
        print "You must specify a location!!"
        exit(-1)
    return args


def main(argv):
    inputs = process_commandline()
    x = paper_cuts(int(inputs.blocksize), 
                   int(inputs.num_of_files),
                   int(inputs.num_of_threads),
                   inputs.location,
                   inputs.file_prefix)
                   
    try:
        if not os.path.isdir(inputs.location):
            os.mkdir(inputs.location)
    except:
        print "Unable to create location:", inputs.location
        exit(0)
    cwd = os.getcwd()
    os.chdir(inputs.location)
    x.start_threads(inputs.private, inputs.multiprocessing)
    x.thread_cleanup()
    x.print_results()
    os.chdir(cwd)


if __name__ == "__main__":
    main(sys.argv[1:])
