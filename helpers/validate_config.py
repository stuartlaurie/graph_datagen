from helpers.general_helpers import *
import logging
import numpy as np

logger=logging.getLogger(__name__)

def set_no_to_generate(no_to_generate):
    if isinstance(no_to_generate,list):
        no_to_generate=np.random.randint(low=no_to_generate[0], high=no_to_generate[1])
    else:
        no_to_generate=string_to_int(no_to_generate) ## allow for string with thousand ,
    return no_to_generate

def add_id_range(idrange,label,start_id,no_to_generate):
    ## store id ranges of nodes/relationships
    idrange[label]={}
    idrange[label]['no_to_generate']=no_to_generate
    idrange[label]['lower']=start_id
    idrange[label]['upper']=no_to_generate+start_id-1
    return idrange

def string_to_int(thing):
    ## converts comma separated sting to int
    if type(thing) != int:
        thing = int(thing.replace(',',''))
    return thing

def validate_config(config):

    idrange={}

    ## allow for string with thousand ,
    config['records_per_file']=string_to_int(config['records_per_file'])

    ## set Dataframe row size to avoid using too much memory when creating larger files
    if 'df_row_limit' not in config:
        config['df_row_limit']=10000000
    else:
        config['df_row_limit']=string_to_int(config['df_row_limit'])

    if 'cycles' not in config:
        config['cycles']=1

    ## node validation
    i=0
    for nodeconfig in config['nodes']:

        if 'start_id' in nodeconfig:
            nodeconfig['start_id']=string_to_int(nodeconfig['start_id'])
        else:
            nodeconfig['start_id']=1

        ## if no_to_generate defined as a list then generate random int in range as no_to_generate
        no_to_generate=set_no_to_generate(nodeconfig['no_to_generate'])

        ## store for lookup of valid id range when creating rels
        idrange=add_id_range(idrange,nodeconfig['label'],nodeconfig['start_id'],no_to_generate)

        ## overwrite config with new values
        config['nodes'][i]=nodeconfig

        i+=1

    ## relationship validation
    i=0
    for relationshipconfig in config['relationships']:

        ## if node label that relationship targets does not exist stop and warn
        if not relationshipconfig['source_node_label'] in idrange:
            logger.error("Relationship source node type: *" + relationshipconfig['source_node_label'] + "* does not exist in node configuration")
            logger.error("Defined node labels for generation are: ")
            logger.error(list(idrange.keys()))
            exit()
        elif not relationshipconfig['target_node_label'] in idrange:
            logger.error("Relationship target node type: *" + relationshipconfig['target_node_label'] + "* does not exist in node configuration")
            logger.error("Defined node labels for generation are: ")
            logger.error(list(idrange.keys()))
            exit()

        if 'no_to_generate' in relationshipconfig:
            ## if no_to_generate defined as a list then generate random int in range as no_to_generate
            no_to_generate=set_no_to_generate(relationshipconfig['no_to_generate'])

        ## allow to specify ratios rather than fixed value, useful to test scaling graphs while only editing node number
        elif 'ratio_to_generate' in relationshipconfig:
            no_to_generate=int(idrange[relationshipconfig['source_node_label']]['no_to_generate']*relationshipconfig['ratio_to_generate'])
        else:
            logger.error("'no_to_generate' or 'ratio_to_generate' are not defined" )
            exit()

        if 'start_id' in relationshipconfig:
            relationshipconfig['start_id']=string_to_int(relationshipconfig['start_id'])
        else:
            relationshipconfig['start_id']=1

        ## store for lookup of valid id range
        idrange=add_id_range(idrange,relationshipconfig['label'],relationshipconfig['start_id'],no_to_generate)

        ## overwrite config with new values
        config['relationships'][i]=relationshipconfig

        i+=1

    return config, idrange


def update_config_ids(config,idrange):

    newidrange={}
    ## node validation
    i=0
    for nodeconfig in config['nodes']:

        nodeconfig['start_id']=nodeconfig['start_id']+idrange[nodeconfig['label']]['no_to_generate']

        ## store for lookup of valid id range when creating rels
        no_to_generate=set_no_to_generate(nodeconfig['no_to_generate'])
        newidrange=add_id_range(idrange,nodeconfig['label'],nodeconfig['start_id'],no_to_generate)

        ## overwrite config with new values
        config['nodes'][i]=nodeconfig

        i+=1

    ## relationship validation
    i=0
    for relationshipconfig in config['relationships']:

        relationshipconfig['start_id']=relationshipconfig['start_id']+idrange[relationshipconfig['label']]['no_to_generate']

        ## store for lookup of valid id range
        no_to_generate=set_no_to_generate(relationshipconfig['no_to_generate'])
        newidrange=add_id_range(idrange,relationshipconfig['label'],relationshipconfig['start_id'],no_to_generate)

        ## overwrite config with new values
        config['relationships'][i]=relationshipconfig

        i+=1

    return config, newidrange
