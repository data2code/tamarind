#!/usr/bin/env python
from tamarind.tamarind import JobManagement, Model
import os,pandas as pd
import argparse as arg

def guess_format(fn):
    """Guess if a file is PDB or CIF"""
    ext=os.path.splitext(fn)[1]
    if ext in ('.gz'):
        #ColabFold cannot use .gz
        #ext=os.path.splitext(fn[:-3])[1]
        return "gz"
    if ext in ('.pdb','.pdb1'): return "pdb"
    if ext in ('.cif'): return "cif"
    with open(fn, mode='rb') as f:
        s=f.read()
    s=s.decode("utf-8", 'ignore')
    S=s.splitlines()
    pat_loop=re.compile(r'^(loop_|data_|_atom)')
    pat_key=re.compile(r'^(HEADER|TITLE|KEYWDS|REMARK|MODEL|END) ')
    for s in S:
        if pat_key.search(s): return 'pdb'
        if pat_loop.search(s): return "cif"
    return ""

class App(Model): # Do not rename the class

    def __init__(self):
        #// Developer: set the job_type exactly
        super().__init__('alphafold')

    #// Set the default parameters from the corresponding online API document
    default_opt={"numModels": "5", "msaMode": "mmseqs2_uniref_env", "numRecycles": "3", "numRelax": False,
            "pairMode":"unpaired_paired", "pdb100Templates":True, "randomSeed":0, "maxMsa": "508:2048",
            "ipsaeScoring":False}

    def run(self, name, seq, output_folder=".", custom_template=None, options=None, wait=True):
        """Set name to your protein name. name should be unique to your account.
        The submission will fail if the name already exists.
        If you want to recompute a previous name, use delete/delete_all first
        """
        opt=AlphaFold.default_opt.copy()
        if options is not None:
            opt.update(options)

        if custom_template is not None:
            if not os.path.exists(custom_template):
                raise Exception(f"file {custom_template} not found!")
            fmt=guess_format(custom_template)
            if fmt!="cif":
                raise Exception(f"file {custom_template} is not a .cif file!")
            self.jm.upload_file(custom_template, f"{name}.cif")
            opt["templateFiles"]=[f"{name}.cif"]
            opt["pdb100Templates"]=False
        print("Settings:", opt)
        opt["sequence"]=seq
        super().run(name, opt, output_folder, wait)

    def batch(self, batch_name, S_name, S_seq, output_folder=".", S_custom_template=None, options=None, wait=True):
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

        # upload template files
        l_template=False
        if S_custom_template is not None:
            if type(S_custom_template) is str:
                S_custom_template=[S_custom_template]*n
            assert(len(S_custom_template)==n)
            l_template=True
            for x in set(S_custom_template):
                if pd.notnull(x) and os.path.exists(x):
                    fmt=guess_format(x)
                    if fmt!="cif":
                        raise Exception(f"file {x} is not a .cif file!")
            c_template=self.jm.upload_batch(batch_name, S_custom_template)

        opt=App.default_opt.copy()
        if options is not None:
            opt.update(options)

        # generate settings
        settings=[]
        jobNames=[]
        for i in range(n):
            one=opt.copy()
            jobNames.append(S_name[i])
            #// Custom logic mostly to be inserted here
            one["sequence"]=S_seq[i]
            if l_template and pd.notnull(S_custom_template[i]) and (S_custom_template[i] in c_template):
                one["templateFiles"]=[c_template[S_custom_template[i]]]
                one["pdb100Templates"]=False
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
                t.sort_values('Rank', ascending=True, inplace=True)
                t['name']=fd
                t['Pdb Path']=t['Pdb Path'].apply(lambda x: os.path.join(output_folder, fd, x))
                out.append(t)
        if len(out):
            t=pd.concat(out, ignore_index=True)
            t.to_csv(os.path.join(output_folder, f"results.csv"), index=False)
            return t
        return None

def main():
    opt = arg.ArgumentParser(description='Run AlphaFold')
    opt.add_argument('-n','--name', type=str, default=None, help='batch name, should be unique.')
    opt.add_argument('-o','--output', type=str, default=".", help='Folder to store results.')
    opt.add_argument('-W','--nowait', action="store_true", help='Exit right after submission without monitoring, you will need to download results later using tmrdownload.')
    opt.add_argument('input', type=str, default=None, help='Input .csv file, require columns: "name", "sequence", "template". Column "template" is optional.')
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
    S_template = t.template.tolist() if 'template' in t.columns else None
    m.batch(args.name, t.name.tolist(), t.sequence.tolist(), output_folder=args.output, S_custom_template=S_template, wait=not args.nowait)

if __name__=="__main__":
    main()
