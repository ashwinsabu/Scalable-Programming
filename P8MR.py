#!/usr/bin/env python

import datetime
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRL3EDRAM(MRJob):

    def mapper_check_error(self, _, line):
        # split the line contents into a list of columns using the space
        columns = line.split(" ")
        #hour and message are extracted
        hour = int(columns[4][11:13])
        message = columns[9:]
        message = " ".join(message)
        # checks if the log line contains L3 EDRAM in it, then extract the duration as it is the 10th column
        if "L3 EDRAM error(s) (dcr 0x0157) detected and corrected" in message:
            try:
                duration = int(columns[9])
                yield hour, duration
            except ValueError:
                pass
    
    def combiner_sum_duration(self, hour, duration):
        # total duration from each mapper is calculated with recording the count of how many events occured to calculate the average
        t_seconds = 0
        count = 0
        for x in duration:
            t_seconds += x
            count += 1
        yield hour, (t_seconds, count)

    def reducer_sum_duration(self, hour, values):
        # Sum the durations and counts from the combiner
        t_seconds = 0
        t_count = 0
        for total_dur, count in values:
            t_seconds += total_dur
            t_count += count
        
        if t_count > 0:
            average_seconds = t_seconds / t_count
        else:
            average_seconds = 0

        yield hour,average_seconds

    def steps(self):
        return [
            # Define the step for the MapReduce job
            MRStep(mapper=self.mapper_check_error, combiner=self.combiner_sum_duration, reducer=self.reducer_sum_duration)
        ]

if __name__ == '__main__':
    MRL3EDRAM.run()

