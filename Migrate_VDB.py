import argparse
import inspect
import json
from json import JSONDecodeError
import socket
import os
import os.path
import datetime
from datetime import datetime
from datetime import date
import sys
from sys import exit
import subprocess
from subprocess import Popen, PIPE
import concurrent.futures
from concurrent.futures import ProcessPoolExecutor
import requests

def enable_environment(engine: str, env_name: str) -> any:
    """"""
    global src_engine, tgt_engine, oper, conffile, dxpath
    oscmd = dxpath + 'dx_ctl_env.exe -d ' + engine + ' -name ' + env_name + ' -action enable -configfile ' + conffile
    return oscmd

def remap_stage_environment(st_dict: dict) -> any:
    """"""
    global src_engine, tgt_engine, oper, conffile, dxpath

    oscmd = dxpath + 'dx_ctl_dsource -d ' + st_dict['Appliance'] + ' -action update -type mssql -validatedsync FULL_OR_DIFFERENTIAL -dsourcename ' + \
        st_dict['dSource'] + ' -stageenv ' + st_dict['stage_env'] + ' -group ' + st_dict['Group'] + ' -stageinst ' + st_dict['stage_instance'] +' -configfile ' + conffile

    return oscmd

def get_dsources(engine: str, env_name: str) -> any:
    """"""
    global src_engine, tgt_engine, oper, conffile, dxpath
    oscmd = dxpath + 'dx_get_db_env -d ' + engine + ' -type dsource -hostenv e -envname ' + env_name + ' -format json -configfile ' + conffile + ' > dsource.json'
    try:
        proc = subprocess.check_call(oscmd, shell=True, stdout=subprocess.DEVNULL)
    except  subprocess.CalledProcessError:
        print("Error fetching dSources: " + env_name)

    slist = list()
    sdict = dict()

    try:
        fTempFile = open("./dsource.json")
        data = json.load(fTempFile)
        fTempFile.close()
    except JSONDecodeError:
        print("No dSources to enable for this environment: " + env_name)
        return slist

    for d in data['results']:
        if d['Enabled'] == 'disabled':
            sdict['engine'] = engine
            sdict['dsource'] = d['Database']
            slist.append(sdict)

    return slist

def enable_dsource(engine: str, d_name: str):
    """"""
    global src_engine, tgt_engine, oper, conffile, dxpath
    oscmd = dxpath + 'dx_ctl_db -d ' + engine + ' -name ' + d_name + ' -action enable -type dsource '  + ' -configfile ' + conffile
    return oscmd

def add_environment(env_name: str,host: str,user_id: str,password: str, toolkitdir: str):
    """"""
    global src_engine, tgt_engine, oper, conffile, dxpath
    oscmd = dxpath + 'dx_create_env -d ' + tgt_engine + ' -envname ' + env_name + ' -envtype windows -host ' + host + ' -username ' + user_id + \
             ' -authtype password -password ' + password + ' -configfile ' + conffile + ' -toolkitdir ' + toolkitdir
    try:
        proc = subprocess.check_call(oscmd, shell=True, stdout=subprocess.DEVNULL)
    except  subprocess.CalledProcessError:
        print("Error adding environment: " + env_name)

def get_replist(engine: str, env_name: str):
    """"""
    global src_engine, tgt_engine, oper, conffile, dxpath

    oscmd = dxpath + 'dx_get_env -d ' + engine + ' -name ' + env_name + ' -replist -configfile ' + conffile + ' > replist.json'
    try:
        proc = subprocess.check_call(oscmd, shell=True, stdout=subprocess.DEVNULL)
    except  subprocess.CalledProcessError:
        print("Error extracting repo list for the: " + env_name)

def get_vdbs(env_name: str) -> any:
    """"""
    global src_engine, tgt_engine, oper, conffile, dxpath

    oscmd = dxpath + 'dx_get_db_env -d ' + src_engine + ' -type vdb -hostenv e -envname ' + env_name + ' -format json -configfile ' + conffile + ' > vdb.json'
    try:
        proc = subprocess.check_call(oscmd, shell=True, stdout=subprocess.DEVNULL)
    except  subprocess.CalledProcessError:
        print("Error fetching dSources: " + env_name)

    slist = list()
    sdict = dict()

    try:
        fTempFile = open("./vdb.json")
        data = json.load(fTempFile)
        fTempFile.close()
    except JSONDecodeError:
        print("No dSources to enable for this environment: " + env_name)
        return slist

    for d in data['results']:
        sdict['sourceDB'] = d['SourceDB']
        sdict['vdb'] = d['Database']
        sdict['env_name'] = d['Env. name']
        sdict['group'] = d['Group']
        sdict['engine'] = d['Appliance']
        """Add this value"""
        sdict['instance'] = d['Instance']
        slist.append(d['Database'])

    return slist

def run_command(command) -> any:
    result = subprocess.run(command,stdout=subprocess.PIPE, text=TRUE)
    return result.stdout

def main():
    global args, src_engine, tgt_engine, oper, conffile, dxpath, bsize
    parser = argparse.ArgumentParser()

    # Add long and short argument
    parser.add_argument("--dxPath", "-dp", default="", help="Path to dxtoolkit")
    parser.add_argument("--confFile", "-cf", default="dxtools.conf", help="File path to dxtools config")
    parser.add_argument("--batchsize", "-bs", default=5, help="Batch processing size. No of executations at a time")
    parser.add_argument("--operation", "-op", required=True,
                        help="1. Enable Environments 2. Relink Stage")
    # Read arguments from the command line
    args = parser.parse_args()
    dxpath2 = args.dxPath
    conffile = args.confFile
    oper = args.operation
    bsize = int(args.batchsize)
    dxpath = dxpath2 + '\\'
    tStamp = datetime.now().strftime("%m%d%Y_%H%M")

    fParmfile = open("./dsources_xref.json")
    data = json.load(fParmfile)
    fParmfile.close()

    engine_dict = dict()

    source_list = list()
    unique_source_list = list()
    stage_list = list()
    unique_stage_list = list()
    engine_list = list()
    unique_engine_list = list()
    link_list = list()
    dsource_list = list()

    for d in data['environments']:
        source_dict = dict()
        stage_dict = dict()
        link_dict = dict()
        stage_dict['engine'] = d['Appliance']
        stage_dict['env_name'] = d['stage_env']
        source_dict['engine'] = d['Appliance']
        source_dict['env_name'] = d['source_env']
        stage_list.append(stage_dict)
        source_list.append(source_dict)
        link_dict['dSource'] = d['dSource']
        link_dict['Appliance'] = d['Appliance']
        link_dict['stage_env'] = d['stage_env']
        link_dict['Group'] = d['Group']
        link_dict['stage_instance'] = d['stage_instance']
        link_dict['source_env'] = d['source_env']

        link_list.append(link_dict)

    unique_source_list = [i for n, i in enumerate(source_list)
                if i not in source_list[:n]]
    unique_stage_list = [i for n, i in enumerate(stage_list)
                if i not in stage_list[:n]]

    if oper == "1":
        """Enable Stage / Target Environments"""
        list_oscmd = list()

        for tgt_item in unique_stage_list:
            list_oscmd.append(enable_environment(tgt_item['engine'],tgt_item['env_name']))

        stage_log = os.getcwd() + '\\report\\stage_enable_' + tStamp + '.txt'

        fReportFile = open(stage_log, "a")
        """Processing in batches"""
        j = 0
        for i in range(0, len(list_oscmd), bsize):
            batch = list_oscmd[i:i + bsize]
            procs = [Popen(j,stdout=subprocess.PIPE, universal_newlines=True) for j in batch]
            for p in procs:
                fReportFile.write("\n***Enabling environment: " + unique_stage_list[j]['engine'] + '/' + unique_stage_list[j]['env_name']+'***\n')
                for stdout_line in iter(p.stdout.readline, ""):
                    fReportFile.write(stdout_line)
                j = j + 1
                p.stdout.close()
                try:
                    p.wait()
                except  subprocess.CalledProcessError:
                    print("error")
        fReportFile.close()
        """Enable Source Environments """
        list_oscmd=list()
        for src_item in unique_source_list:
            list_oscmd.append(enable_environment(src_item['engine'],src_item['env_name']))

        source_log = os.getcwd() + '\\report\\source_enable_' + tStamp + '.txt'

        fReportFile = open(source_log, "a")
        """Processing in batches"""
        j = 0
        for i in range(0, len(list_oscmd), bsize):
            batch = list_oscmd[i:i + bsize]
            procs = [Popen(j, stdout=subprocess.PIPE, universal_newlines=True) for j in batch]
            for p in procs:
                fReportFile.write("\n***Enabling environment: " + unique_source_list[j]['engine'] + '/' + unique_source_list[j]['env_name']+'***\n')
                for stdout_line in iter(p.stdout.readline, ""):
                    fReportFile.write(stdout_line)
                j = j + 1
                p.stdout.close()
                try:
                    p.wait()
                except  subprocess.CalledProcessError:
                    print("error")
        fReportFile.close()

    elif oper == "2":
        """Relinking Stage Host"""
        list_oscmd = list()
        procs = list()
        for t_u_item in link_list:
            list_oscmd.append(remap_stage_environment(t_u_item))

        remap_log = os.getcwd() + '\\report\\remap_' + tStamp + '.txt'

        fReportFile = open(remap_log, "a")
        """Processing in batches"""
        j = 0
        for i in range(0, len(list_oscmd), bsize):
            batch = list_oscmd[i:i + bsize]
            procs = [Popen(j, stdout=subprocess.PIPE, universal_newlines=True) for j in batch]
            for p in procs:
                fReportFile.write("\n***Relinking to new stage: "+link_list[j]['Appliance']+'/'+link_list[j]['stage_env'] + \
                    ' to the source: ' + link_list[j]['Appliance'] + '/' + link_list[j]['source_env'] + '***\n')
                for stdout_line in iter(p.stdout.readline, ""):
                    fReportFile.write(stdout_line)
                j = j + 1
                p.stdout.close()
                try:
                    p.wait()
                except  subprocess.CalledProcessError:
                    print("error")

        procs.clear()
        list_oscmd.clear()

        """"Enable the dSources after linking"""

        for t_d_item in link_list:
            list_oscmd.append(enable_dsource(t_d_item['Appliance'],t_d_item['dSource']))

        j = 0
        for i in range(0, len(list_oscmd), bsize):
            batch = list_oscmd[i:i + bsize]
            procs = [Popen(j, stdout=subprocess.PIPE, universal_newlines=True) for j in batch]
            for p in procs:
                fReportFile.write(
                    "\n***Enable dSource: " + link_list[j]['dSource'] + ' on the stage environment ' + link_list[j]['Appliance'] + '/' + \
                    link_list[j]['stage_env'] + '***\n')
                for stdout_line in iter(p.stdout.readline, ""):
                    fReportFile.write(stdout_line)
                j = j + 1
                p.stdout.close()
                try:
                    p.wait()
                except  subprocess.CalledProcessError:
                    print("error")

        fReportFile.close()

    """
    elif oper == "3":
        ""Get List of dSource on all Source environments""
        for src_item in unique_source_list:
            dsource_list = dsource_list + get_dsources(src_item['engine'],src_item['env_name'])

        ""Enable dSources""
        for d_item in dsource_list:
            enable_dsource(d_item['engine'],d_item['dsource'])
    elif oper == "4":
        ""Add target environments""
        for t_a_item in target_add_list:
            add_environment(t_a_item['name'],t_a_item['new_host'],t_a_item['user'],t_a_item['pass'],t_a_item['tookitdir'])
    elif oper == "5":
        ""Add target environments""
        for item in source_eng_list:
            vdb_list = vdb_list +get_vdbs(item)
    """
    sys.stdout.flush()

if __name__ == '__main__':
    main()