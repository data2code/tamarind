#!/usr/bin/env python
import requests
import pandas as pd
import os,random,string,time,json,tqdm,zipfile,re

proxies = {k:os.environ.get(k+"_proxy") for k in ("http","https") if os.environ.get(k+"_proxy", "")!=""}
# interval (seconds) when pulling job status
MONITOR_INTERVAL=10
DEBUG=False

class JobManagement:

    def __init__(self, api_key=None, base_url="https://app.tamarind.bio/api/"):
        self.api_key = api_key or os.environ.get("TAMARIND_API_KEY", None)
        if self.api_key is None:
            print("ERROR> API key not found, please set it with environment variable TAMARIND_API_KEY")
            exit()
        self.base_url = base_url
        self.headers = {'x-api-key': self.api_key}

    def generate_temp_job_name(self, length=6):
        """Generate a temporary job name of six characters"""
        characters = string.ascii_lowercase + string.digits
        while True:
            temp_job_name = ''.join(random.choice(characters) for _ in range(length))
            t=self.get_jobs(job_name=temp_job_name)
            if len(t)==0: break
        return temp_job_name

    def submit_job(self, job_name, job_type, settings):
        """Submit a single job"""
        endpoint = "submit-job"
        params = {
            "jobName": job_name,
            "type": job_type,
            "settings": settings
        }
        response = requests.post(self.base_url + endpoint, headers=self.headers, json=params)
        if DEBUG: print(response.text)
        if response.status_code!=200:
            raise Exception(f"Job {job_name} cannot be created! "+response.text)
        return response.text

    def submit_batch(self, batch_name, job_type, settings):
        """Submit a batch containing multiple jobs.
        Notice batchName and type within settings, if exist, will be overwritten.
        """
        endpoint = "submit-batch"
        settings["batchName"] = batch_name
        settings["type"] = job_type
        response = requests.post(self.base_url + endpoint, headers=self.headers, json=settings)
        if DEBUG: print(response.text)
        return response.text

    def upload_file(self, local_filepath, uploaded_filename, folder=None):
        """Upload a local file to cloud, optionally under a user-specified folder."""
        endpoint = f"upload/{uploaded_filename}"
        params = {}
        if folder is not None:
            params["folder"] = folder
        with open(local_filepath, 'rb') as file_data:
            response = requests.put(self.base_url + endpoint, headers=self.headers, data=file_data, params=params)
        return response.status_code

    def upload_batch(self, batch_name, S_local_filepath, empty_first=True):
        """Create a folder named [batch_name], and place multiple local files inside
        If folder is not specified, they are placed into root folder.
        Redudant local files will only be uploaded once.

        empty_first: if True, delete existing remote files in the folder

        return dict mapping the local file name to the remote file path
        """
        uploaded=set()
        c_upload={}
        if empty_first:
            self.delete_batch_files(batch_name)
        else:
            uploaded=set(self.get_files(folder = batch_name))
        for x in set(S_local_filepath):
            if pd.isnull(x) or x=='': continue
            fn, ext = os.path.splitext(os.path.basename(x))
            name = fn+ext
            full_name=os.path.join(batch_name, name)
            i=2
            while full_name in uploaded:
                name= fn+str(i)+ext
                full_name=os.path.join(batch_name, name)
                i+=1
            self.upload_file(x, name, folder=batch_name)
            c_upload[x]=full_name
            uploaded.add(full_name)
        if DEBUG:
            print("Uploading files: ", c_upload)
        return c_upload

    def is_batch(self, job_name):
        """Check if a job_name is a job name or a batch name"""
        t=get_jobs(job_name=job_name)
        if len(t)==0:
            raise Exception(f"Job {job_name} is not found!")
        return dict(t.iloc[0])['Type']=='batch'

    def get_jobs(self, **kw):
        """Get a dataframe containing job entries

        job_name: default None means all jobs
        expand_batch: default False means return a batch job as one single entry, otherwise, return underlying jobs
        organization: default False means only return own jobs
        job_type: restricted to job_type
        """
        opt={"job_name": None, "expand_batch": False, "organization": False, "job_type": None}
        if kw is not None:
            opt.update(kw)
        endpoint = "jobs"
        params={}
        if opt['job_name'] is not None:
            params['jobName']=opt['job_name']
        if opt['organization']:
            params['organization']=True
        if opt['expand_batch']:
            params['includeSubjobs']="true"
        out=[]
        while True:
            response = requests.get(self.base_url + endpoint, headers=self.headers, params=params)
            if response.status_code!=200:
                if DEBUG: print(response.text)
                break
            jobs_json=response.json()
            if DEBUG: print(jobs_json)

            if 'jobs' in jobs_json:
                out.extend(jobs_json['jobs'])
            elif '0' in jobs_json:
                out.append(jobs_json['0'])
            if 'startKey' not in jobs_json:
                break
            # more pages
            params['startKey']=jobs_json['startKey']
        if len(out)==0:
            jobs_df=pd.DataFrame([], columns=['Score','JobName','JobStatus','Type','Settings','Created','Model','Batch'])
            return jobs_df
        else:
            jobs_df=pd.DataFrame(out)
            jobs_df['Model']=jobs_df.apply(lambda r: r.Settings if r.Type=='batch' else r.Type, axis=1)
        if opt['job_type']:
            x=opt['job_type']
            jobs_df=jobs_df[jobs_df.Model==x].copy()
        if 'Batch' not in jobs_df.columns:
            jobs_df['Batch']=jobs_df.apply(lambda r: r.JobName if r.Type=='batch' else None, axis=1)
        else: # expanded, let's remove the batch entry
            jobs_df=jobs_df[jobs_df.Type!='batch'].copy()
        return jobs_df

    def get_batch_jobs(self, batch_name):
        """Get all jobs listed under a batch submission"""
        endpoint = "jobs"
        params={"batch": batch_name}
        out=[]
        while True:
            response = requests.get(self.base_url + endpoint, headers=self.headers, params=params)
            if response.status_code != 200:
                print(f"Warning: no job found under batch {batch_name}")
                if DEBUG: print(response.text)
                break
            jobs_json=response.json()
            if DEBUG: print(jobs_json)
            if 'jobs' in jobs_json:
                out.extend(jobs_json['jobs'])
            elif '0' in jobs_json:
                out.append(jobs_json['0'])
            if 'startKey' not in jobs_json:
                break
            params['startKey']=jobs_json['startKey']
        if len(out)==0:
            jobs_df=pd.DataFrame([], columns=['Score','JobName','JobStatus','Type','Settings','Created','Batch'])
        else:
            jobs_df=pd.DataFrame(out)
        return jobs_df

    def delete_job(self, job_name):
        """Delete a job entry"""
        endpoint = "delete-job"
        params = {"jobName": job_name}
        response = requests.post(self.base_url + endpoint, headers=self.headers, json=params)
        print(f"Job deleted: {job_name}")
        if DEBUG: print(response.text)
        return response.text

    def delete_batch(self, batch_name):
        """Delete all jobs under a batch"""
        t=self.get_batch_jobs(batch_name)
        # delete all uploaded files
        self.delete_batch_files(batch_name)
        for i,r in t.iterrows():
            self.delete_job(r['JobName'])
        self.delete_job(batch_name)
        print(f"Batch deleted: {batch_name}")

    def delete_all_jobs(self, **kw):
        """Delete all jobs.
           kw takes the same values as the method get_jobs(), i.e., job_name, expand_batch, organization, job_type
        """
        kw['expand_batch'] = False
        t=self.get_jobs(**kw)
        for i,r in t.iterrows():
            if r['Type']=='batch':
                self.delete_batch(r['JobName'])
            else:
                self.delete_job(r['JobName'])
        t=self.get_jobs(**kw)
        if len(t)==0:
            print("All jobs have been deleted successfully.")
            return True
        else:
            print(f"{len(t)} jobs cannot be deleted: "+", ".join(t[:5].JobName.tolist()))
            return False

    def get_results(self, job_name, output_folder="."):
        """Save job output into output_folder
        jobs_name: use get_batch_results, if you have a batch_name
        output_folder: output folder name, defaults to the current folder
        """
        endpoint = "result"
        params = {"jobName": job_name}
        response = requests.post(self.base_url + endpoint, headers=self.headers, json=params)
        if response.status_code == 200:
            if DEBUG: print(response.text)
            results_url = response.text.replace('"', '')
            results_response = requests.get(results_url, proxies=proxies)
            if results_response.status_code == 200:
                # when the sequence name appears before in other batches, Tamarind adds batch name as prefix
                # we prefer to remove that
                job_name=re.sub(r'[^\-]+-', '', job_name)
                fout=os.path.join(output_folder, job_name)
                os.makedirs(fout, exist_ok=True)
                save_path = os.path.join(fout, "result.zip")
                with open(save_path, 'wb') as file:
                    file.write(results_response.content)
                with zipfile.ZipFile(save_path, "r") as zip_file:
                    zip_file.extractall(fout)
                os.remove(save_path)
                return f"Downloaded and unpack results into: {fout}"
            else:
                return f"Failed to download results: {results_response.status_code}"
        else:
            if DEBUG: print(response.text)
            return f"Failed to retrieve results URL: {response.status_code}"

    def get_batch_results(self, batch_name, output_folder="."):
        """Save all job outputs into output_folder, each job entry has its own subfolder"""
        t=self.get_batch_jobs(batch_name)
        output_folder=os.path.join(output_folder, batch_name)
        if len(t)==0: return
        pg=tqdm.tqdm(total=len(t), position=0)
        for i,r in t.iterrows():
            self.get_results(r['JobName'], output_folder=output_folder)
            pg.update(1)

    def get_files(self, folder=None):
        """Get a list of files from root, or a specific folder"""
        endpoint = "files"
        params = {}
        if folder is not None:
            params["folder"] = folder
        response = requests.get(self.base_url + endpoint, headers=self.headers, params=params)
        if DEBUG: print(response.text)
        return response.json()

    def get_all_files(self):
        """Get a list of all files, including files under folders"""
        endpoint = "files"
        params = {"includeFolders": "true"}
        response = requests.get(self.base_url + endpoint, headers=self.headers, params=params)
        if DEBUG: print(response.text)
        return response.json()

    def delete_file(self, file_path):
        """Delete a specific file"""
        endpoint = "delete-file"
        params = { "filePath": file_path }
        response = requests.get(self.base_url + endpoint, headers=self.headers, params=params)
        if DEBUG: print(response.text)
        return response.json()

    def delete_batch_files(self, batch_name):
        """Delete all files under a folder named batch_name"""
        endpoint = "delete-file"
        params = { "folder": batch_name}
        response = requests.get(self.base_url + endpoint, headers=self.headers, params=params)
        if DEBUG: print(response.text)
        return response.json()

    def delete_all_files(self):
        """Delete all files uploaded by user"""
        for file_name in self.get_all_files():
            if file_name.endswith("/"): # a folder
                self.delete_batch_files(file_name[:-1])
            else:
                self.delete_file(file_name)

    def monitor(self, job_name, output_folder=".", skip_download=False):
        """Check the status of a job, waits till it is completed. Then save results to the output_folder.

        output_folder: path to the local folder that stores the results
        skip_download: if False, do not download results
        """
        N=5
        n=0
        pg=tqdm.tqdm(total=N, position=0)
        while True:
            t=self.get_jobs(job_name=job_name)
            if len(t)==0:
                raise Exception(f"Job {job_name} is missing!")
            status=t.loc[0, 'JobStatus']
            #status='Running'
            n+=1
            if status=='Complete':
                if not skip_download:
                    self.get_results(job_name, output_folder)
                pg.update(N)
                print(f"Job {job_name} is completed")
                break
            elif status=='Stopped':
                raise Exception(f"Job {job_name} is stopped!")
            else: # we increase the progress bar
                if n/N>=2/3:
                    N*=2
                    pg.total=N
                    pg.refresh()
            pg.update(1)
            pg.set_description(status)
            time.sleep(MONITOR_INTERVAL)


    def monitor_all(self, job_type=None, job_names=None, expand_batch=True, output_folder=".", skip_download=False):
        """Monitor multiple jobs as described by job_type, job_names, expand_batch
        job_type: jobs with certain job_type, default None - means any
        job_names: a list of job names
        expand_batch: include jobs within batches, True as the default
        """

        if job_names is not None:
            job_names=set(job_names)
        pg=None
        c_downloaded=set()
        while True:
            t=self.get_jobs(job_type=job_type, expand_batch=expand_batch)
            if job_names is not None:
                t=t[t.JobName.isin(job_names)]
            N=len(t)
            n=len(t[t.JobStatus.isin(('Complete','Stopped'))])
            if pg is None:
                pg=tqdm.tqdm(total=N, position=0)
            else:
                if pg.total!=N:
                    pg.total=N
                    pg.refresh()
            pg.update(max(n-pg.n, 0))
            if not skip_download:
                t_new=t[(t.JobStatus=='Complete') & (t.JobName.notin(c_downloaded))]
                for i,r in t_new.iterrows():
                    self.get_results(r['JobName'], output_folder)
                    c_downloaded.add(r['JobName'])
            if N==n:
                del pg
                print({k:len(t_v) for k,t_v in t.groupby('JobStatus')})
                break
            time.sleep(MONITOR_INTERVAL)

    def monitor_batch(self, batch_name, output_folder=".", skip_download=False):
        """Monitor all jobs within a batch, save output to output_folder"""
        pg=None
        c_downloaded=set()
        while True:
            t=self.get_batch_jobs(batch_name)
            N=len(t)
            n=len(t[t.JobStatus.isin(('Complete','Stopped'))])
            if pg is None:
                pg=tqdm.tqdm(total=N, position=0)
            else:
                if pg.total!=N:
                    pg.total=N
                    pg.refresh()
            pg.update(max(n-pg.n,0))
            for i,r in t.iterrows():
                if r['JobStatus']=='Complete' and r['JobName'] not in c_downloaded:
                    if not skip_download:
                        self.get_results(r['JobName'], output_folder)
                        c_downloaded.add(r['JobName'])
            if N==n:
                del pg
                print({k:len(t_v) for k,t_v in t.groupby('JobStatus')})
                break
            time.sleep(MONITOR_INTERVAL)

class Model:

    job_type=None

    def __init__(self, job_type, api_key=None):
        Model.job_type=job_type
        self.jm=JobManagement(api_key)
        self.job_name=None
        self.batch_name=None

    def get_options(self, options = None):
        opt = self.__class__.default_opt.copy()
        if options is not None:
            opt.update(options)
        return opt

    def no_duplicate(self, s_name, S_name):
        n_dup=len(S_name)-len(set(S_name))
        if n_dup>0:
            print(f"ERROR> There are {n_dup} duplicate entries in {s_name}")
            c_seen=set()
            for x in S_name:
                if x in c_seen:
                    print(x)
                c_seen.add(x)
            exit()

    def upload_templates(self, S_name, S_tmpl, batch_name=None):
        """ S_name, list of sequence names
            S_tmpl, corresponding list of template file path. For one sequence name, there can be multiple
                template files, which should be ";"-concatenated into one string
            E.g., S_name=["a","b"], S_tmpl=["a_tmpl1.cif;a_tmpl2.cif", "b_tmpl.cif"]
            if S_tmpl is a string, we assume it's shared by all entries

            If batch_name is None, there should only be one entry in S_name, all attachments
            are uploaded to the root folder.

            return c_template, where keys are entries in S_name, values are list of uploaded file names
        """
        # Make sure size of S_name and S_tmpl are the same
        if S_tmpl is None: return {}
        if type(S_name) is str:
            S_name=[S_name]
            if type(S_tmpl) is not str:
                # make sure they are treated as one entry
                S_tmpl=";".join([x for x in S_tmpl if pd.notnull(x)])
        n=len(S_name)
        if batch_name is None and n!=1:
            raise Exception(f"S_name must be one entry, if no batch_name is provided.")
        if type(S_tmpl) is str:
            S_tmpl=[S_tmpl]
        if n>1 and len(S_tmpl)==1:
            S_tmpl=S_tmpl*len(S_name)
        if n!=len(S_tmpl):
            raise Exception(f"Length of S_name mismatch S_tmpl: {n} vs {len(S_tmpl)}")
        S_tmpl=['' if pd.isnull(x) else x for x in S_tmpl ]

        for i,s in enumerate(S_tmpl):
            S_fn =list({ x.strip() for x in re.split(r';\s*', s) })
            S_tmpl[i]=S_fn
        S_unique=list({ x for X in S_tmpl for x in X if x!=''})
        c_template={}
        # check file format
        for x in S_unique:
            if not os.path.exists(x):
                raise Exception(f"file {x} not found!")
            fmt=guess_format(x)
            if fmt!="cif":
                raise Exception(f"file {x} is not a .cif file!")
        # upload
        if batch_name is not None:
            c_map=self.jm.upload_batch(batch_name, S_unique)
        else:
            c_map={}
            uploaded=set()
            for x in set(S_unique):
                fn, ext = os.path.splitext(os.path.basename(x))
                name = fn+ext
                i=2
                while name in uploaded:
                    name= fn+str(i)+ext
                    i+=1
                self.jm.upload_file(x, name)
                c_map[x]=name
                uploaded.add(name)

        c_template=[]
        for i,X in enumerate(S_tmpl):
            c_template.append(sorted([c_map[x] for x in X if x in c_map]))
        return c_template

    def run(self, job_name=None, settings=None, output_folder=".", wait=True):
        """Run a single job"""
        self.job_name=job_name or self.jm.generate_temp_job_name()
        self.batch_name=None
        assert(settings is not None)
        out=self.jm.submit_job(self.job_name, Model.job_type, settings)
        if not wait:
            self._notify(self.job_name, output_folder, False)
            return self.job_name
        self.jm.monitor(self.job_name, output_folder=output_folder)
        self._notify(self.job_name, output_folder, True)

    def batch(self, batch_name=None, settings=None, output_folder=".", wait=True):
        """Run a batch of jobs"""
        self.batch_name=batch_name or self.jm.generate_temp_job_name()
        self.job_name=None
        assert(settings is not None)
        out=self.jm.submit_batch(self.batch_name, Model.job_type, settings)
        if not wait:
            self._notify(self.batch_name, output_folder, False)
            return self.batch_name
        self.jm.monitor_batch(self.batch_name, output_folder)
        self._notify(self.batch_name, output_folder, True)

    def delete(self):
        if self.job_name is not None:
            self.jm.delete_job(self.job_name)
        if self.batch_name is not None:
            self.jm.delete_batch(self.batch_name)

    def _notify(self, name, output_folder=".", wait=True):
        if wait:
            print(f"Job completed, outputs in {output_folder}.\nPlease delete the batch with:\n    tmrdeljob {name}")
        else:
            s=f"Job submitted as: {name}."
            s+=f"\nTo monitor the progress:\n    tmrmonitor {name}"
            s+=f"\nWhen completed, download results with:\n    tmrdownload -o {output_folder} {name}"
            s+=f"\nTo delete the batch:\n    tmrdeljob {name}"
            print(s)

    def download_batch(self, batch_name, output_folder="."):
        self.jm.get_batch_results(batch_name, output_folder)

    def download(self, job_name, output_folder="."):
        self.jm.get_results(job_name, output_folder)

def parse_json(json_string):
    try:
        if json_string is None:
            return None
        return json.loads(json_string)
    except json.JSONDecodeError as e:
        print(f"Invalid JSON format: {e.msg}")
        print(f"Error at line {e.lineno}, column {e.colno}")
        print(f"Problematic part: {e.doc[e.pos-20:e.pos+20]}")
        sys.exit(1)

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

def main():
    args = parse_arguments()
    settings_dict = parse_json(args.setting)
    print("Parsed JSON as dictionary:")
    print(settings_dict)

if __name__ == "__main__":
    main()

if __name__=="__main__":
    # Example usage:
    jm = JobManagement()
    #jm.upload_file("1crn.cif", "111.cif", folder="my")
    #jm.get_jobs(job_type="alphafold").display()
    #print(jm.get_all_files())
    #exit()
    print(jm.get_jobs())
    exit()
    print(jm.get_files(folder="mybatch"))
    print(jm.get_files(folder="mybatch-1crn1"))
    print(jm.get_files(folder="msas/mybatch/"))
    exit()

    t=jm.get_jobs(expand_batch=True)
    t.to_csv('t.csv')
    exit()

    c=jm.upload_batch("alphafold", ["1crn.cif"])
    print(c)
    exit()

    # Submit a job
    job_name = "myJobName"
    job_type = "alphafold"
    settings = {
        "sequence": "MALKSLVLLSLLVLVLLLVRVQPSLGKETAAAKFERQHMDSSTSAASSSNYCNQMMKSRNLTKDRCKPVNTFVHESLADVQAVCSQKNVACKNGQTNCYQSYSTMSITDCRETGSSKYPNCAYKTTQANKHIIVACEGNPYVPVHFDASV"
    }

    print(jm.submit_job(job_name, job_type, settings))

    # Upload a file
    local_filepath = "/path/to/your/file1.txt"
    uploaded_filename = "file1.txt"
    print(jm.upload_file(local_filepath, uploaded_filename))

    # Get jobs
    print(jm.get_jobs())

    # Delete a job
    print(jm.delete_job(job_name))

    # Get results
    print(jm.get_results(job_name))

    # Get files
    print(jm.get_files())

