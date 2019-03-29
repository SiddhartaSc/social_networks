
# coding: utf-8

# # GeoSpatial DeepDive Module
# 
# This module is designed to qickly analyze and visualize geospatial information

# In[1]:


#FOLIUM MAPS
import folium
from folium import FeatureGroup, LayerControl


# ## Visualizations

# In[17]:


def put_pins_map(point_list, center_start=[22.493569, -102.114636], zoom_start=5):
    '''
    Given a list of [latitude,longitude] returns a map with the plotted points over a world map.
    Default center and zoom displays Mexico
    '''
    #Folium map centered in Mexico
    map_ = folium.Map(center_start, zoom_start=zoom_start)
    
    folium.TileLayer('stamenterrain').add_to(map_)
    folium.TileLayer('Mapbox Bright').add_to(map_)
    folium.TileLayer('cartodbpositron').add_to(map_)
    folium.TileLayer('cartodbdark_matter').add_to(map_)
    
    # add pins
    feature_group = FeatureGroup(name = "Pins")
    for pin in point_list:
        folium.CircleMarker(pin,fill =True, color='#3186cc',radius=2,
                        fill_color='#3186cc').add_to(feature_group)
    feature_group.add_to(map_)
    
    #Save map
    LayerControl().add_to(map_)
    file_path = 'pins_map.html'
    map_.save(outfile = file_path)
    
    return map_


# ## Examples

# In[20]:


list_of_points = [[25.6749285,-100.3095586], [25.6849285,-101.3095586], [22.493569, -102.114636]]
put_pins_map(list_of_points)

