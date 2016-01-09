# import logging
#
# import numpy as np
#
# logger = logging.getLogger(__name__)
#
#
# class CatalogManager():
#     """
#     Class implementing catalog manager to handle metadata
#     """
#
#     def __init__(self):
#         """
#         Constructor for catalog manager. It just initializes a dictionary for catalog
#         """
#         # initialize an empty dictionary to hold metadata
#         self.metadata_catalog = {}
#
#     def get_metadata(self, obj, metadata_name):
#         """
#
#         Parameters
#         ----------
#         obj: Pandas DataFrame
#             DataFrame for which the metadata should be retrieved
#
#         metadata_name: str
#             Name of the metadata. Typically it is 'ltable', 'rtable', 'foreign_key_ltable', 'foreign_key_rtable'
#
#         Returns
#         -------
#         value: object
#             Value for the given metadata name
#
#         """
#         # get metadata information for the given object
#         d = self._get_obj_catalog(obj)
#
#         # return the value for the requested key
#         return self._get_metadata_from_obj_catalog(d, metadata_name)
#
#     def set_metadata(self, obj, name, value):
#         """
#
#         Parameters
#         ----------
#         obj: Pandas DataFrame
#             DataFrame for which the metadata should be set
#
#         name: str
#             Name of the metadata. Typically it will be 'ltable', 'rtable', 'foreign_key_ltable', 'foreign_key_rtable'
#         value: str or Pandas DataFrame
#             Value to be set
#
#         Returns
#         -------
#         status : bool
#             Returns True if the updation was successful
#
#         """
#         if self.is_obj_in_catalog(obj) == False:
#             d = {}
#             self._set_metadata_to_obj_catalog(d, name, value)
#             self.metadata_catalog[id(obj)] = d
#         else:
#             d = self._get_obj_catalog(obj)
#             self._set_metadata_to_obj_catalog(d, name, value)
#
#
#     def set_key(self, obj, key_attr_name):
#         """
#
#         Parameters
#         ----------
#         obj: Pandas DataFrame
#             DataFrame for which the key should be set
#
#         key_attr_name: str
#             Column name to be set as key
#
#         Returns
#         -------
#         status: boolean
#             Whether the function successfully set the key
#
#         Notes
#         -----
#         Key attribute column is expected to satisfy the following properties
#
#         * It should not contain duplicate values
#         * It should not contain null (missing) values
#
#         """
#         # check if the attribute name is a string
#         if self.is_string(key_attr_name) == False:
#             raise AttributeError('Attribute name must be a string')
#         if self._is_key_attr(obj, key_attr_name) == True:
#             self.set_metadata(obj, 'key', key_attr_name)
#         else:
#             raise AssertionError(key_attr_name + ' does not qualify to be a key attribute')
#         return True
#
#     def get_key(self, obj):
#         """
#         Get key for a dataframe
#
#         Parameters
#         ----------
#         obj : Pandas DataFrame
#             Dataframe for which key has to be retrieved
#
#         Returns
#         -------
#         key : str
#             Key attribute name
#         """
#         d = self._get_obj_catalog(obj)
#         return self._get_metadata_from_obj_catalog(d, 'key')
#
#     def del_metadata(self, obj, metadata_name):
#         # delete metadata
#
#         if self.is_obj_in_catalog(obj) == False:
#             logger.info('Object not in the catalog')
#             return False
#         d = self._get_obj_catalog(obj)
#         if self.is_metadata_in_catalog(d, metadata_name):
#             del d[metadata_name]
#         return True
#
#     def del_all_metadata(self, obj):
#         # delete all metadata
#         if self.is_obj_in_catalog(obj) == False:
#             logger.info('Object not in the catalog')
#             return False
#         del self.metadata_catalog[id(obj)]
#         return True
#
#
#
#
#     def is_obj_in_catalog(self, obj):
#         """
#         Check whether the dataframe object is in catalog
#
#         Parameters
#         ----------
#         obj : Pandas DataFrame
#             Dataframe that should be checked for its presence
#
#         Returns
#         -------
#         present_flag : bool
#             Returns True if the Dataframe is present, else returns False
#
#         """
#         return id(obj) in self.metadata_catalog
#
#     def is_metadata_in_catalog(self, catalog, metadata_name):
#         """
#         Check whether the given metadata is present for an object in the catalog (for that object)
#
#         Parameters
#         ----------
#         catalog : dict
#             Dictionary with metdata information.
#         metadata_name : str
#             metadata in the catalog
#
#         Returns
#         -------
#
#         """
#         return metadata_name in catalog
#
#
#     # helper functions
#
#     def is_string(self, s):
#         if not isinstance(s, basestring):
#             return False
#         else:
#             return True
#
#     def _get_obj_catalog(self, obj):
#         # get the id for the df object
#         obj_id = id(obj)
#
#         # check if the catalog contains the df object
#         if obj_id in self.metadata_catalog == False:
#             # obj not in the catalog
#             raise KeyError('Object with id ' + str(obj_id) + ' not in the catalog.')
#         else:
#             # obj is present in the catalog
#             # check if the metadata name is in the obj's metdata
#             d = self.metadata_catalog[obj_id]
#             return d
#
#     def _get_metadata_from_obj_catalog(self, obj_catalog, name):
#         if self.is_string(name) == False:
#             raise AttributeError('Metadata name must be a string')
#
#         if name not in obj_catalog:
#             # metadata name is not in obj's catalog
#             raise KeyError('Metadata ' + name + " is not in object's catalog")
#         else:
#             # metadata name is in obj's catalog
#             return obj_catalog[name]
#
#     def _set_metadata_to_obj_catalog(self, obj_catalog, name, value):
#         if self.is_string(name) == False:
#             raise AttributeError('Metadata name must be a string')
#         obj_catalog[name] = value
#
#     # check whether an attribute can be set as key
#     def _is_key_attr(self, df, attr_name):
#         """
#
#         Parameters
#         ----------
#         df : Pandas DataFrame
#             DataFrame to which key metadata must be added.
#
#         attr_name : str
#             Column name in df
#
#         Returns
#         -------
#         status : bool
#             True, if the column for the given attribute is a key column.
#
#         """
#         if attr_name not in list(df.columns):
#             logger.error('Dataframe does not contain ' + attr_name + ' column')
#
#         # check if the length is > 0
#         if len(df) > 0:
#             # check for uniqueness
#             uniq_flag = len(np.unique(df[attr_name])) == len(df)
#             if not uniq_flag:
#                 logger(__name__).warning('Attribute contains duplicate values')
#
#             # check if there are missing or null values
#             nan_flag = sum(df[attr_name].isnull()) == 0
#             if not nan_flag:
#                 logger(__name__).warning('Attribute contains missing values')
#             return (uniq_flag and nan_flag)
#         else:
#             return True
