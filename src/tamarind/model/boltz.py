#!/usr/bin/env python
import tamarind.tamarind as tmr
from tamarind.tamarind import JobManagement, Model
import os,pandas as pd,re
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

    def run(self, name, seq, custom_template=None, output_folder=".", options=None, wait=True):
        """Set name to your protein name. name should be unique to your account.
        The submission will fail if the name already exists.
        If you want to recompute a previous name, use delete/delete_all first
        """
        opt = self.get_options(options)

        S_tmpl = self.upload_templates(name, custom_template)[0]
        if len(S_tmpl):
            opt["templateFiles"] = S_tmpl[0]

        opt["sequence"]=seq
        super().run(name, opt, output_folder, wait)

    def batch(self, batch_name, S_name, S_seq, S_custom_template=None, output_folder=".", options=None, wait=True):
        opt = self.get_options(options)
        n=len(S_seq)
        assert(len(S_name)==n)
        self.no_duplicate("S_name", S_name)

        S_tmpl = self.upload_templates(S_name, S_custom_template, batch_name)
        # merge all templates, as they are shared within a batch
        S_tmpl = sorted(list({x for X in S_tmpl for x in X if x!=''}))

        def load_json(opt, pdb_id):
            import util_bzhou
            from string import Template
            template = Template(opt['__json__'])
            fn_json = template.substitute(pdb_id=pdb_id)
            s = util_bzhou.DumpObject.load_json(fn_json)
            del opt['__json__']
            opt.update(s)
            # print(opt)
            return opt

        settings=[]
        jobNames=[]
        for i in range(n):
            one=opt.copy()
            if '__json__' in one:
                one = load_json(one,S_name[i])
            jobNames.append(S_name[i])
            #// Custom logic mostly to be inserted here
            one["sequence"]=S_seq[i]
            if len(S_tmpl):
                one["templateFiles"]=S_tmpl
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
    opt.add_argument('--setting', type=str, default=None, help='JSON string that overwrites the default model settings')
    opt.add_argument('--debug', action='store_true', help='Print message from the API')
    opt.add_argument('-W','--nowait', action="store_true", help='Exit right after submission without monitoring, you will need to download results later using tmrdownload.')
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
    # boltz model share the same templates per batch
    S_template = t.template.tolist() if 'template' in t.columns else None
    if S_template is not None:
        S=[x for x in S_template if pd.notnull(x)]
        S_template=list(set(x.strip() for x in re.split(r';\s*', ";".join(S))))
        print(f"Custom templates provided: {len(S_template)}.")
    m.batch(args.name, t.name.tolist(), t.sequence.tolist(), S_custom_template=S_template, output_folder=args.output, options=opt,)
    print(f"Job completed, outputs in {args.output}.\nPlease delete the batch with: deljob.py {args.name}")

if __name__=="__main__":
    main()

