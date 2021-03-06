from PIL import Image
from io import BytesIO
import numpy as np
from matplotlib import cm

def array2rgb(data, cm_name='RdYlGn', num_level=256):
    cmap = cm.get_cmap(cm_name, num_level)
    rescaled = (1.0 / data.max() * (data - data.min()))
    rgb = cmap(rescaled)
    rgb = np.uint8(rgb*255)
    # with BytesIO() as f:
    #     img.save(f, format='JPEG')
    #     f.seek(0)
    #     img_jpg = Image.open(f)
    #     rgb = np.asarray(img_jpg)
    return rgb