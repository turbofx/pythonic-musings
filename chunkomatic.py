#!/usr/bin/python

# Chunk-o-matic is a stupid name, I'll think of a better one later
# This program is designed to allow for the incremental download and reassembly of a large file
# without having to preprocess the file into a bunch of chunks. The only preprocessing is the creation
# of the map file.

import os, sys, hashlib, ConfigParser
from optparse import OptionParser

DEFAULT_CHUNK_SIZE = 536870912 # 500MB
DEFAULT_BLOCK_SIZE = 32768 # default block read size
DEFAULT_CHUNK_LABEL = 'c' # label for each chunk piece.
DEFAULT_HASH_TYPE = 'md5' # yeah, yeah, I know.
DEFAULT_TEMP_FILE = 'chunkomatic.tempfile'

DEBUG = True

def debug(msg):
    if DEBUG:
        print "XX DEBUG XX: ", msg

class chunkomatic(object):
    def __init__(self, default_chunk_size=DEFAULT_CHUNK_SIZE, default_block_size = DEFAULT_BLOCK_SIZE):
        self.default_chunk_size = default_chunk_size
        self.default_block_size = default_block_size
        self.default_chunk_label = DEFAULT_CHUNK_LABEL
        self.default_hash_type = DEFAULT_HASH_TYPE

    def setup_mapconfig(self):
        ''' Create a ConfigParser object and add our default options '''
        self.mapfile_config = ConfigParser.RawConfigParser()
        self.mapfile_config.add_section('Defaults')
        self.mapfile_config.set('Defaults', 'default_chunk_size', self.default_chunk_size)
        self.mapfile_config.set('Defaults', 'default_block_size', self.default_block_size)
        self.mapfile_config.set('Defaults', 'default_chunk_label', self.default_chunk_label)
        self.mapfile_config.set('Defaults', 'default_hash_type', self.default_hash_type)

    def digest_dir(self, dir2process):
        ''' Process a directory of files at this point we don't handle subdirs '''
        cwd = os.getcwd()
        debug("Digesting dir: %s" % os.path.abspath(dir2process))
        os.chdir(dir2process)
        for f in os.listdir(dir2process):
            if os.path.isfile(os.path.abspath(f)):
                debug("Digesting file: %s" % f)
                result = self.digest_file(f)
                if result != 0:
                    debug("There was a problem processing file: %s" % f)
            else:
                debug("%s is NOT a file... ignoring!" % f)
        os.chdir(cwd)

    def digest_file(self, file2process):
        ''' 
        Read in a file and create an entry in the mapfile containing the full path
        and chunk offset checksumming information
        '''
        #1 Let's try to open the file!
        try:
            fp = os.open(os.path.abspath(file2process), os.O_RDONLY)
        except:
            # log error here
            debug("Unable to open file: %s for reading!" % file2process)
            return -1
        section_name = 'file:%s' % os.path.abspath(file2process)
        self.mapfile_config.add_section(section_name)
        fsize = os.stat(os.path.abspath(file2process)).st_size
        self.mapfile_config.set(section_name, 'fsize', fsize)
        fchunks = int(fsize / self.default_chunk_size) + 1
        self.mapfile_config.set(section_name, 'fchunks', fchunks)
        debug("File will generate %d checksum chunks" % fchunks)
        chunk_num = 0 # what chunk we're currently in
        chunk_label = '%s%d' % (self.default_chunk_label, chunk_num) # The zero chunk
        start_offset = 0 # where we started in the file
        cur_offset = 0 # where we are in the file
        chunk_offset = 0 # where we are in the chunk
        chunk_hash = hashlib.new(self.default_hash_type) # Create the hash for the chunk
        total_hash = hashlib.new(self.default_hash_type) # Create the hash for the whole file
        debug("Processing chunk: %d" % chunk_num) 
        while(chunk_num < fchunks):
            hchunk = os.read(fp, self.default_block_size) # read a chunk from the file
            if len(hchunk) > 0: # we're not at end of file, yet
                chunk_hash.update(hchunk) # update the hash
                total_hash.update(hchunk) # update the file hash
                cur_offset += len(hchunk) # increment our file offset
                chunk_offset += len(hchunk) # increment where we are in teh file
                if chunk_offset >= self.default_chunk_size: # we've hit the end of a chunk
                    debug("Chunk boundary hit! [%d : %s]" % (cur_offset, chunk_hash.hexdigest()))
                    self.mapfile_config.set(section_name, chunk_label, "%d %d %s %s" % 
                                            (start_offset, cur_offset, chunk_hash.hexdigest(), total_hash.hexdigest()))
                    # reset the chunk hash
                    chunk_hash = hashlib.new(self.default_hash_type)
                    chunk_num += 1
                    chunk_label = '%s%d' % (self.default_chunk_label, chunk_num)
                    chunk_offset = 0
                    start_offset = cur_offset # Move the start offset up for the next chunk
            else: # len(hchunk) is <= 0 thus EOF
                os.close(fp)
                self.mapfile_config.set(section_name, chunk_label, "%d %d %s %s" % 
                                        (start_offset, cur_offset, chunk_hash.hexdigest(), total_hash.hexdigest())) # write chunk
                self.mapfile_config.set(section_name, 'file_checksum', "%s" % total_hash.hexdigest()) # wrote total file checksum
                break
        debug("Processing of file %s is complete!" % file2process)
        return 0

    def get_filesection(self, file2fetch):
        for sec in self.mapfile_config.sections():
            if sec.startswith('file:'): # Do we have a file section?
                if os.path.split(file2fetch)[1] == os.path.split(sec.split(':')[1])[1]: # is this the file we're looking for?
                    debug("We have a match!")
                    return sec
        return None # we got through all the sections and didn't find the file we were looking for.
    
    def get_realfilename(self, section):
        return os.path.split(section.split(':')[1])[1]

    def gen_tempfilename(self, location, section, c_num):
        return os.path.join(location, os.path.split(self.get_realfilename(section))[1] + "." + c_num)

    def fetch_dir(self, origin_location, location2store):
        for sec in self.mapfile_config.sections():
            if sec.startswith('file:'):
                debug("Processing section: %s" % sec)
                self.fetch_file(os.path.split(sec.split(':')[1])[1], origin_location, location2store)
                
    def fetch_file(self, file2fetch, origin_location, location2store):
        section = self.get_filesection(file2fetch)
        if section == None:
            debug("File %s is not in map.")
            return False
        print "Section: %s" % section
        success = 0
        failure = 0
        temp_clist = {} # dictonary containing the key=position, value=tempfilename
        for items in self.mapfile_config.items(section):
            print "%s = %s" % (items[0], items[1])
            if items[0].startswith(self.default_chunk_label):
                if self.fetch_chunk(section, origin_location, location2store, items) == 0:
                    temp_clist[items[0]] = self.gen_tempfilename(location2store, section, items[0])
                    debug("Chunk was successfully fetched!")
                    success += 1
                else:
                    debug("FAILURE to fetch chunk!")
                    failure += 1
        debug("Some assembly required... batteries not included.")
        if failure == 0:
            cwd = os.getcwd()
            os.chdir(os.path.abspath(location2store))
            debug("All chunks were fetched successfully!")
            self.assemble_chunks(temp_clist, section)
            os.chdir(cwd)
        else:
            debug("Failed chunks: %d\nSuccessful: %d" % (failure, success))
    
    def assemble_chunks(self, chunk_dict, section, verify=True):
        ''' 
        This function takes the chunk dictionary and uses to assemble the original_file
        '''
        real_filename = self.get_realfilename(section)
        try:
            fp = os.open(real_filename, os.O_WRONLY|os.O_CREAT)
        except:
            debug("Unable to open %s for writing!" % os.path.abspath(real_filename))
            return -1
        for key in sorted(chunk_dict.keys()):
            c_fp = os.open(chunk_dict[key], os.O_RDONLY)
            done = False
            while not done:
                data_chunk = os.read(c_fp, self.default_block_size)
                if len(data_chunk) != 0:
                    if os.write(fp, data_chunk) != len(data_chunk):
                        debug("Error reassembling file!!!")
                        os.close(fp)
                        os.close(c_fp)
                        return -1
                else:
                    os.close(c_fp)
                    done = True
                    debug("Done integrating chunk: %s" % key)
        os.close(fp)
        debug("Done assembling!")
        if verify:
            if self.verify_chunk(real_filename, self.mapfile_config.get(section, 'file_checksum')):
                debug("File verified!")
                return 0
            else:
                debug("File verified FAILED!")
                return -1
    
    def fetch_chunk(self, section, origin, location, chunk_info):
        # first thing, let's see that we can actually open the origin file...
        real_filename = self.get_realfilename(section)
        debug("real_filename: %s" % real_filename)
        try:
            origin_fp = os.open(os.path.join(origin, real_filename), os.O_RDONLY)
        except:
            debug("Unable to open origin file for reading: %s" % os.path.join(origin, os.path.split(real_filename)))
            return -1
        cwd = os.getcwd()
        os.chdir(location) # let's move into our assembly area
        c_num = chunk_info[0] # what is our chunk number?
        start_loc = int(chunk_info[1].split()[0]) # what's are start location in the file?
        end_loc = int(chunk_info[1].split()[1]) # end location?
        chunk_checksum = chunk_info[1].split()[2] # chunk checksum?
        temp_filename = self.gen_tempfilename(location, section, c_num)
        if os.path.exists(temp_filename): # Have we already processed this chunk?
            if self.verify_chunk(temp_filename, chunk_checksum):
                return 0 # this chunk is done. otherwise, let's keep going
        try:
            chunk_fp = os.open(temp_filename, os.O_WRONLY|os.O_CREAT)
        except:
            debug("Unable to open temporary location for writing: %s" % temp_filename)
            os.chdir(cwd)
            return -1
        computed_checksum = hashlib.new(self.default_hash_type)
        if os.lseek(origin_fp, start_loc, os.SEEK_SET) != start_loc:
            debug("Unable to seek to chunk position in file...")
            os.close(origin_fp)
            os.close(chunk_fp)
            os.chdir(cwd)
            return -1
        cur_loc = start_loc
        while cur_loc < end_loc:
            data_block = os.read(origin_fp, self.default_block_size)
            if len(data_block) != 0: # we didn't hit end of file.
                computed_checksum.update(data_block) # update the checksum
                bw = os.write(chunk_fp, data_block) # write the datablock to the file
                if bw != len(data_block):
                    debug("ERROR! %d written instead of %d!" % (bw, len(data_block)))
                    os.close(origin_fp)
                    os.close(chunk_fp) # I'm not going to delete in case they're data that can be salvaged.
                    os.chdir(cwd)
                    return -1
                cur_loc += len(data_block)
            else: # we hit end of file
                os.close(origin_fp)
                break # at this point we're at the end.
        if computed_checksum.hexdigest() != chunk_checksum: # We have a problem!
            debug("ERROR! Computed checksum %s does NOT match: %s" % (computed_checksum.hexdigest(), chunk_checksum))
            os.chdir(cwd)
            return -1
        debug("Verifying chunk!")
        if self.verify_chunk(temp_filename, chunk_checksum):
            os.chdir(cwd)
            return 0 # right as rain
        else:
            os.chdir(cwd)
            return -1

    def verify_chunk(self, file2check, chunk_checksum):
        try:
            fp = os.open(file2check, os.O_RDONLY)
        except:
            debug("Unable to open CHUNK file: %s" % file2check)
        done = False
        computed_checksum = hashlib.new(self.default_hash_type)
        while not done:
            data_block = os.read(fp, self.default_block_size)
            if len(data_block) != 0:
                computed_checksum.update(data_block)
            else:
                done = True
        if computed_checksum.hexdigest() == chunk_checksum:
            return True
        else:
            return False
        
    def write_mapfile(self, mapfile):
        with open(mapfile, 'wb') as mp:
            self.mapfile_config.write(mp)

    def load_mapfile(self, mapfile):
        try:
            fp = open(mapfile, 'rb')
        except:
            debug("Unable to open map file!")
            exit(-1)
        debug("Loading map file: %s" % mapfile)
        self.mapfile_config = ConfigParser.RawConfigParser()
        self.mapfile_config.readfp(fp)
        fp.close()
        self.load_defaults()

    def load_defaults(self):
        self.default_chunk_size = self.mapfile_config.getint('Defaults', 'default_chunk_size')
        self.default_block_size = self.mapfile_config.getint('Defaults', 'default_block_size')
        self.default_chunk_label = self.mapfile_config.get('Defaults', 'default_chunk_label')
        self.default_hash_type = self.mapfile_config.get('Defaults', 'default_hash_type')
        
    def verify_location(self, location):
        ''' Verify the location that files will be deposited into is writeable, etc. '''
        cwd = os.getcwd()
        try:
            os.chdir(location) # is it a directory we can enter?
        except:
            debug("Unable to enter directory: %s" % location)
            return False
        try:
            fp = open(os.path.join(location, DEFAULT_TEMP_FILE), 'w')
            fp.write("This is a test")
            fp.close()
        except:
            debug("Unable to create a file in: %s" % location)
            return False
        try:
            os.unlink(os.path.join(location, DEFAULT_TEMP_FILE))
        except:
            debug("Unable to delete temp file: %s from location %s" % (DEFAULT_TEMP_FILE, location))
            return False
        os.chdir(cwd)
        return True

def process_cli():
    parser = OptionParser()
    parser.add_option("-m", "--mapfile", dest="mapfile", help="Mapfile that will be used. Mandatory option")
    parser.add_option("-c", "--create", dest="create", action="store_true", help="Create mode. Must have -f or -d options")
    parser.add_option("-p", "--process", dest="process", action="store_true", help="Process mode. Must have -l option and either -d or -f")
    parser.add_option("-s", "--single", dest="single", help="Process a single chunk. Must have -f and -l options")
    parser.add_option("-f", "--file", dest="file", help="Process a single file")
    parser.add_option("-d", "--dir", dest="directory", help="Process a directory")
    parser.add_option("-l", "--location", dest="location", help="Location that processed files will be placed.")
    parser.add_option("-o", "--origin", dest="origin", help="Location to look for source files to download from.")
    (options, args) = parser.parse_args()
    # You must specify a mapfile.
    if not options.mapfile:
        print "You MUST specify a mapfile!"
        exit(-1)
    # You have to choose a mode... eithe create or process
    if (not options.create) and (not options.process):
        print "You MUST specify a mode!"
        exit(-1)
    # If you chose process you must also pick a location
    if options.process and (not options.location):
        print "You MUST specify a location for files to be outputted to."
        exit(-1)
    return (options, args)
    

def main(argv):
    x = chunkomatic()
    (options, args) = process_cli()
    if options.create:
        debug("In create mode.")
        x.setup_mapconfig() # set our defaults
        if options.file: # process a single file
            debug("Processing a single file")
            if x.digest_file(options.file) != 0:
                debug("There was a problem processing file: %s" % options.file)
                exit(-1) # no point even writing the mapfile... we failed to process our single file...
        if options.directory: # process a single dir
            x.digest_dir(options.directory)
        x.write_mapfile(options.mapfile)
    elif options.process:
        debug("In Process mode.")
        x.load_mapfile(options.mapfile)
        if not x.verify_location(options.location):
            debug("Verify of target location failed...")
            exit(-1)
        if options.file:
            x.fetch_file(options.file, options.origin, options.location)
        elif options.directory:
            x.fetch_dir(options.directory, options.location)
    
if __name__ == '__main__':
    main(sys.argv[1:])
