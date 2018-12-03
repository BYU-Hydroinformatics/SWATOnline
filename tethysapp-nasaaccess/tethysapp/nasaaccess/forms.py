from django.forms import ModelForm
from .model import Shapefiles, DEMfiles, accessCode



class UploadShpForm(ModelForm):
    class Meta:
        model = Shapefiles
        fields = ('shapefile',)

class UploadDEMForm(ModelForm):
    class Meta:
        model = DEMfiles
        fields = ('DEMfile',)

class accessCodeForm(ModelForm):
    class Meta:
        model = accessCode
        fields = ('access_code',)