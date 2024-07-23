#!/usr/bin/env python

import datetime
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRInvalidProgramImageCount(MRJob):

    def mapper_check_invalid_images(self, _, line):
        # split the line contents into a list of columns using the space
        columns = line.split(" ")
        month = int(columns[2][5:7])
        message = columns[9:]
        message = " ".join(message)
        if month == 1 or month == 12:
            if "invalid or missing program image," in message:
                yield None, 1
    
    def combiner_count_images(self, _, counts):
        # sum the count of invalid or missing image in the mappers
        yield None, sum(list(counts))

    def reducer_count_invalid_images(self, _, counts):
        # Sum the occurrences of the specific log message
        yield "Occurrences of the message are", sum(list(counts)) 

    def steps(self):
        return [
            # Define the step for the MapReduce job
            MRStep(mapper=self.mapper_check_invalid_images,combiner=self.combiner_count_images, reducer=self.reducer_count_invalid_images)
        ]

if __name__ == '__main__':
    MRInvalidProgramImageCount.run()
