import numpy as np
import astropy.io.fits as pf
from astropy.time import Time
import astropy.units as u

def cross_spectrum(hdulist, b, s):
    idxb = hdulist['UV_DATA'].data['BASELINE']==b
    idxs = hdulist['UV_DATA'].data['SOURCE']==s
    idx = idxb==idxs * idxb

    time=hdulist['UV_DATA'].data['TIME'][idx]
    # central frequencies
    if hdulist['FREQUENCY'].header['NO_BAND'] > 1:
        freq = np.zeros((hdulist['FREQUENCY'].header['NO_BAND'],
                         hdulist['FREQUENCY'].header['NO_CHAN']))
        for i in range(hdulist['FREQUENCY'].header['NO_BAND']):
            if hdulist['FREQUENCY'].data['SIDEBAND'][0][i]==1:
                freq[i,:] = (hdulist['FREQUENCY'].header['REF_FREQ']
                             +hdulist['FREQUENCY'].data['BANDFREQ'][0][i]
                             +hdulist['SOURCE'].data['FREQOFF'][
                        hdulist['SOURCE'].data['SOURCE_ID.']==s][0][i] +
                             (1+np.arange(hdulist['FREQUENCY'].header['NO_CHAN'])
                              -hdulist['FREQUENCY'].header['REF_PIXL']) *
                             hdulist['FREQUENCY'].data['CH_WIDTH'][0][i])
            elif hdulist['FREQUENCY'].data['SIDEBAND'][0][i]==-1:
                freq[i,:] = (hdulist['FREQUENCY'].header['REF_FREQ']
                             +hdulist['FREQUENCY'].data['BANDFREQ'][0][i]+
                             hdulist['SOURCE'].data['FREQOFF'][
                        hdulist['SOURCE'].data['ID_NO.']==s][0][i] +
                             (1+hdulist['FREQUENCY'].header['NO_CHAN']
                              -hdulist['FREQUENCY'].header['REF_PIXL']-
                              (1+np.arange(hdulist['FREQUENCY'].header['NO_CHAN'])))
                             *hdulist['FREQUENCY'].data['CH_WIDTH'][0][i])
            else:
                print( 'Invalid frequency' )
                raise Exception
    else:
        if hdulist['FREQUENCY'].data['SIDEBAND']==1:
            freq = (hdulist['FREQUENCY'].header['REF_FREQ']
                    +hdulist['FREQUENCY'].data['BANDFREQ']+
                    hdulist['SOURCE'].data['FREQOFF'][
                    hdulist['SOURCE'].data['ID_NO.']==s] +
                    (1+np.arange(hdulist['FREQUENCY'].header['NO_CHAN'])
                     -hdulist['FREQUENCY'].header['REF_PIXL']) *
                    hdulist['FREQUENCY'].data['CH_WIDTH'])
        elif hdulist['FREQUENCY'].data['SIDEBAND']==-1:
            freq = (hdulist['FREQUENCY'].header['REF_FREQ']
                    +hdulist['FREQUENCY'].data['BANDFREQ']+
                    hdulist['SOURCE'].data['FREQOFF'][
                    hdulist['SOURCE'].data['ID_NO.']==s] +
                    (1+hdulist['FREQUENCY'].header['NO_CHAN']
                     -hdulist['FREQUENCY'].header['REF_PIXL']-
                     (1+np.arange(hdulist['FREQUENCY'].header['NO_CHAN'])))
                    *hdulist['FREQUENCY'].data['CH_WIDTH'])
        else:
            print( 'Invalid frequency' )
            raise Exception
        freq = np.array([freq]).squeeze()
    freq = freq/1.0e6
    I=hdulist['UV_DATA'].data['FLUX'][idx]
    I=I.flatten().reshape((len(time),hdulist['FREQUENCY'].header['NO_BAND'],hdulist['FREQUENCY'].header['NO_CHAN'],hdulist['UV_DATA'].header['MAXIS2'],hdulist['UV_DATA'].header['MAXIS1\
']))
    return I,time,freq
