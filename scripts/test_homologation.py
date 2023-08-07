


from easyocr import Reader
import argparse
import cv2
import matplotlib.pyplot as plt

def cleanup_text(text):
	# strip out non-ASCII text so we can draw the text on the image
	# using OpenCV
	return "".join([c if ord(c) < 128 else "" for c in text]).strip()

# break the input languages into a comma separated list
langs = "en".split(",")
print("[INFO] OCR'ing with the following languages: {}".format(langs))
# load the input image from disk
image = cv2.imread("image_page1_I1.jpg")
#image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
# OCR the input image using EasyOCR
print("[INFO] OCR'ing input image...")
reader = Reader(langs, gpu=0 > 0)
results = reader.readtext(image)

name = "image_page1_I1.jpg".split("/")[-1].split(".")[-2]
texts = []

# loop over the results
for (bbox, text, prob) in results:
	# display the OCR'd text and associated probability
	texts.append(text)
	print("[INFO] {:.4f}: {}".format(prob, text))
	# unpack the bounding box
	(tl, tr, br, bl) = bbox
	tl = (int(tl[0]), int(tl[1]))
	tr = (int(tr[0]), int(tr[1]))
	br = (int(br[0]), int(br[1]))
	bl = (int(bl[0]), int(bl[1]))
	# cleanup the text and draw the box surrounding the text along
	# with the OCR'd text itself
	text = cleanup_text(text)
	cv2.rectangle(image, tl, br, (0, 255, 0), 2)
	cv2.putText(image, text, (tl[0], tl[1] - 10),
	cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 255, 0), 2)

with open(f"image_page1_I1.jpg_file_{name}.txt", 'w') as f:
    f.write('\n'.join(texts))

# show the output image
cv2.imshow('Image', image)  # Provide a window name and the image to be displayed
cv2.waitKey(0)  # Wait until a key is pressed (0 means indefinite)
cv2.destroyAllWindows()  # Close all open windows

# If you're using Jupyter Notebook, you can add the following line to avoid freezing the kernel
#plt.show()



