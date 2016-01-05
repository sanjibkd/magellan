import logging

logger = logging.getLogger(__name__)


class CatalogManager(object):
    def __init__(self):
        self.metadata = {}

    # methods to implement
    # 1. get_property, set_property
    # 2. get_dict, delete_dict
    # 3. get_all_properties

    # set the property
    def set_property(self, object, property_name, property_value):
        object_id = id(object)
        if self.metadata.has_key(object_id) == True:
            logger.info('Catalog contains the object')
            prop_dict = self.metadata[object_id]
            if prop_dict.has_key(property_name):
                logger.warning('Property dict for the object already contains ' + property_name)
                logger.warning('Updating the property value')
                prop_dict[property_name] = property_value
            else:
                logger.info('Property dict for the object DOES not contain ' + property_name)
                prop_dict[property_name] = property_value
            self.metadata[object_id] = prop_dict
        else:
            logger.info('Catalog does not contain the object')
            prop_dict = {}
            prop_dict[property_name] = property_value
            self.metadata[object_id] = prop_dict

    def get_property(self, object, property_name):
        object_id = id(object)
        if self.metadata.has_key(object_id) == False:
            raise KeyError('Catalog does not contain the object')
        prop_dict = self.metadata[object_id]
        if prop_dict.has_key(property_name) == False:
            raise KeyError('Property dict for the object does not contain ' + property_name)
        return prop_dict[property_name]

    def get_dict(self, object):
        object_id = id(object)
        if self.metadata.has_key(object_id) == False:
            raise KeyError('Catalog does not contain the object')
        return self.metadata[object_id]

    def del_dict(self, object):
        object_id = id(object)
        if self.metadata.has_key(object_id) == False:
            raise KeyError('Catalog does not contain the object')
        del self.metadata[object_id]
