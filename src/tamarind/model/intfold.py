#!/usr/bin/env python
import sys,os
import tamarind.tamarind as tmr
from tamarind.tamarind import JobManagement, Model
import pandas as pd,re
import argparse as arg

class App(Model): # Do not rename the class

    def __init__(self):
        #// Developer: set the job_type exactly
        super().__init__('intfold')

    #// Set the default parameters from the corresponding online API document
    default_opt={"inputFormat": "sequence", "numSamples": 5, "numBatches": "1", "seed": 0,
            "numRecycles": 10, "outputType": "pdb"}

    def run(self, name, seq, output_folder=".", options=None, wait=True):
        """Set name to your protein name. name should be unique to your account.
        The submission will fail if the name already exists.
        If you want to recompute a previous name, use delete/delete_all first
        """
        opt = self.get_options(options)
        opt["sequence"]=seq
        super().run(name, opt, output_folder, wait)

    def batch(self, batch_name, S_name, S_seq, output_folder=".", options=None, wait=True):
        opt = self.get_options(options)
        n=len(S_seq)
        assert(len(S_name)==n)
        self.no_duplicate("S_name", S_name)

        # generate settings
        settings=[]
        jobNames=[]
        for i in range(n):
            one=opt.copy()
            jobNames.append(S_name[i])
            #// Custom logic mostly to be inserted here
            one["sequence"]=S_seq[i]
            settings.append(one)
        params = {
            "batchName": batch_name,
            "type": App.job_type,
            "settings": settings,
            "jobNames": jobNames
        }

        super().batch(batch_name, params, output_folder, wait)
        #// If we need to compile a result.csv file
        self.results(output_folder)

    @staticmethod
    def results(output_folder):
        if not os.path.exists(output_folder):
            return
        jobs=os.listdir(output_folder)
        out=[]
        for fd in jobs:
            #// This should be overwritten depending on the model ouput
            fn=os.path.join(output_folder, fd, "result.csv")
            if os.path.exists(fn):
                t=pd.read_csv(fn)
                t.sort_values('ranking_score', ascending=False, inplace=True)
                t['name']=fd
                t['Pdb Path']=t['filename'].apply(lambda x: os.path.join(output_folder, fd, x))
                out.append(t)
        if len(out):
            t=pd.concat(out, ignore_index=True)
            t.to_csv(os.path.join(output_folder, f"results.csv"), index=False)
            return t
        return None

def main():
    opt = arg.ArgumentParser(description='Run IntFold')
    opt.add_argument('-n','--name', type=str, default=None, help='batch name, should be unique.')
    opt.add_argument('-o','--output', type=str, default=".", help='Folder to store results.')
    opt.add_argument('--setting', type=str, default=None, help='JSON string that overwrites the default model settings')
    opt.add_argument('--debug', action='store_true', help='Print message from the API')
    opt.add_argument('-W','--nowait', action="store_true", help='Exit right after submission without monitoring, you will need to download results later using tmrdownload.')
    opt.add_argument('input', type=str, default=None, help='Input .csv file, require columns: "name", "sequence", "template". Column "template" is optional.')
    args=opt.parse_args()
    if args.debug:
        tmr.DEBUG = True
    opt=tmr.parse_json(args.setting)
    m = App()
    jobs = m.jm.get_jobs(job_name=args.name)
    if len(jobs)>0:
        print(f"Error> Job name {args.name} already exists!")
        exit()
    t = pd.read_csv(args.input)
    for col in ['name','sequence']:
        if col not in t.columns:
            print(f"ERROR> missing required column {col}.")
    m.batch(args.name, t.name.tolist(), t.sequence.tolist(), output_folder=args.output, options=opt, wait=not args.nowait)

if __name__=="__main__":
    main()
