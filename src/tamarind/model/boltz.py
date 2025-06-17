#!/usr/bin/env python
from tamarind.tamarind import JobManagement, Model
import os,pandas as pd
import argparse as arg

class App(Model): # Do not rename the class

    def __init__(self):
        #// Developer: set the job_type exactly
        super().__init__('boltz')

    #// Set the default parameters from the corresponding online API document
    # versions are 2.1.1, 1.0.0, 0.4.0
    default_opt={"inputFormat": "sequence", "addLigands": False, "numSamples": 5, "numBatches": 1,
            "numRecycles": 3, "predictAffinity": False, "binderChain": "A", "stepScale": 1.638,
            "seed":0, "bonds": "", "pocketRestraints":"", "outputType":"pdb", "version": "2.1.1",
            "templateFiles": [], "templateMapping": []
            }

    def run(self, name, seq, output_folder=".", options=None, wait=True):
        """Set name to your protein name. name should be unique to your account.
        The submission will fail if the name already exists.
        If you want to recompute a previous name, use delete/delete_all first
        """
        opt=App.default_opt.copy()
        if options is not None:
            opt.update(options)

        opt["sequence"]=seq
        super().run(name, opt, output_folder, wait)

    def batch(self, batch_name, S_name, S_seq, output_folder=".", options=None, wait=True):
        n=len(S_seq)
        assert(len(S_name)==n)
        n_dup=len(S_name)-len(set(S_name))
        if n_dup>0:
            print(f"ERROR> There are {n_dup} duplicate names in S_name")
            c_seen=set()
            for x in S_name:
                if x in c_seen:
                    print(x)
                c_seen.add(x)
            exit()

        opt=App.default_opt.copy()
        if options is not None:
            opt.update(options)

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
            fn=os.path.join(output_folder, fd, "metrics.csv")
            if os.path.exists(fn):
                t=pd.read_csv(fn)
                t.sort_values('iptm', ascending=False, inplace=True)
                t['name']=fd
                t['pdb_filepath']=t['pdb_filepath'].apply(lambda x: os.path.join(output_folder, fd, x))
                # there is a bug in pdb_filepath, so we fix it ourselves for now, will delete when it's fixed
                t['pdb_filepath']=t['pdb_filepath'].apply(lambda x: x.replace('result_result_', 'result_'))
                out.append(t)
        if len(out):
            t=pd.concat(out, ignore_index=True)
            t.to_csv(os.path.join(output_folder, f"results.csv"), index=False)
            return t
        return None

def main():
    #// Update the job_type below
    opt = arg.ArgumentParser(description='Run Boltz')
    opt.add_argument('-n','--name', type=str, default=None, help='batch name, should be unique.')
    opt.add_argument('-o','--output', type=str, default=".", help='Folder to store results.')
    opt.add_argument('input', type=str, default=None, help='Input .csv file, require columns: "name", "sequence".')
    args=opt.parse_args()
    m = App()
    jobs = m.jm.get_jobs(job_name=args.name)
    if len(jobs)>0:
        print(f"Error> Job name {args.name} already exists!")
        exit()
    t = pd.read_csv(args.input)
    for col in ['name','sequence']:
        if col not in t.columns:
            print(f"ERROR> missing required column {col}.")
    m.batch(args.name, t.name.tolist(), t.sequence.tolist(), output_folder=args.output)
    print(f"Job completed, outputs in {args.output}.\nPlease delete the batch with: deljob.py {args.name}")

if __name__=="__main__":
    main()

