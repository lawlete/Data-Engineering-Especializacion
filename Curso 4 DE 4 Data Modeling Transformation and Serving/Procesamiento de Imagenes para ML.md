::: {.cell .code id="xNERg7WTFQYV"}
``` python
import tensorflow as tf
import matplotlib.pyplot as plt
import tensorflow_datasets as tfds
```
:::

:::::::::::: {.cell .code colab="{\"base_uri\":\"https://localhost:8080/\",\"height\":588,\"referenced_widgets\":[\"df6600060a89459e845f884fa0edb3bf\",\"0f4dff32349047a4bccd1617e72222d3\",\"a5354158173d4552b171d06507f33e1d\",\"56ec4d45d44c4510b349ec007d0cab23\",\"fd52b5c4b6714a0cbc5218def98d842b\",\"09b53eb0cf90494083200066b0dcda96\",\"d1ae2b06fddd4c7581c54a59542fa35f\",\"3841e31dc27846a582bd62b31a0c545c\",\"332dfdfdd4524c0a9c98ce944d083094\",\"de09ca027a91429995660d43dbad4042\",\"9f7c891c6ba54b9497b5c3a7256188ff\",\"bbc261c3697e4e869ad01bf18be68f09\",\"72a3e0f4d5b54dc6a16ef4ccf94ef7d8\",\"f7cddd4a2fef4ac5934d8c158a54e2f7\",\"512c3a96aa5046938988554f24eeb94d\",\"15f1b49de21d4c3984643b4e74e2c8cb\",\"9e07618d5daf41a4860017057134503b\",\"ec64effc0138496397461766bcce89a8\",\"845a70cfc37347d3b59ca79c7138a833\",\"b2e6fe0739f04c558e37b5f44f431bae\",\"987ddd33fae14a77bc99efed9baf0136\",\"b5aa88047fe84312858c6c722eede336\",\"caab90086ea74bcc81249131efdc37fe\",\"2b315f860e1e446dbb3ac2375ded102a\",\"7656ab1a0ec7439089c54a83d89c35e4\",\"74793bc5f0a4425a9c5ec002613f564c\",\"19cb849480764ad28c440291839e49ad\",\"eb022c7aee3a42cfb559b9210087cb9d\",\"37d4de881fde416a9179ae56202acff0\",\"8edd2e2aecac456db95ec40c8af13476\",\"03225f2acb42495099f6ea5f8c94ae3d\",\"ec4005ddac414e95bc458db58088a9b8\",\"b621b42a63d84a0d87ba54cdcb0d047b\",\"da0d81e996d64d1d9730defea8bd7db1\",\"1533a7c915354ce8b5fbeaa012411149\",\"d9938dcf899942fd8f5b33ee7431f656\",\"f7febc13d024458384aaf982ad7986dd\",\"31c010b837df4e8a9889c9a37e5bb6dd\",\"9b6bf652832b407c91fe3dba4a025851\",\"71bb30d27f394a838bcec79e0d2e6048\",\"cc0bb69e2dc8498294b637ba6fbd163c\",\"8778a45a01cf4be6a900df48585db65d\",\"1b7f001ace1e4a33af709082ed122994\",\"85b253d09c6b4732a67c315d50751ca0\",\"9e23f8bb3e164becbd0d66adbb26047a\",\"8fb434b2572d4bfface6226e920d71a9\",\"f9aa0b47a7c44f6a8402c345b17b1844\",\"e85dd73227334f6a9c6057dc7dc195f9\",\"b747590d76c44e8dbcd01956289d8d5c\",\"c0d55e9967f343958ee1a6306d1e8632\",\"8be3f4949c974d4a8096a16c762e35ab\",\"2841d4c75f46499390230963c5ce3ff3\",\"1b10f1973c9746d69885cf8b36d94f1d\",\"95220958396f4281963e65bef1de46a5\",\"bc66696d514e479ebf0b8e3f2a1b137f\"]}" id="4iFxwn9vQ9ln" outputId="c925ff7a-5ddb-4d01-9fd3-55698b513f53"}
``` python
dataset = tfds.load('cats_vs_dogs', split='train', as_supervised=True)

for image, label in dataset.take(1):
  plt.imshow(image)
  print(image.shape, label)
  print(label)
```

::: {.output .stream .stdout}
    Downloading and preparing dataset 786.67 MiB (download: 786.67 MiB, generated: 1.04 GiB, total: 1.81 GiB) to /root/tensorflow_datasets/cats_vs_dogs/4.0.1...
:::

::: {.output .display_data}
``` json
{"model_id":"df6600060a89459e845f884fa0edb3bf","version_major":2,"version_minor":0}
```
:::

::: {.output .display_data}
``` json
{"model_id":"bbc261c3697e4e869ad01bf18be68f09","version_major":2,"version_minor":0}
```
:::

::: {.output .display_data}
``` json
{"model_id":"caab90086ea74bcc81249131efdc37fe","version_major":2,"version_minor":0}
```
:::

::: {.output .display_data}
``` json
{"model_id":"da0d81e996d64d1d9730defea8bd7db1","version_major":2,"version_minor":0}
```
:::

::: {.output .stream .stderr}
    WARNING:absl:1738 images were corrupted and were skipped
:::

::: {.output .display_data}
``` json
{"model_id":"9e23f8bb3e164becbd0d66adbb26047a","version_major":2,"version_minor":0}
```
:::

::: {.output .stream .stdout}
    Dataset cats_vs_dogs downloaded and prepared to /root/tensorflow_datasets/cats_vs_dogs/4.0.1. Subsequent calls will reuse this data.
    (262, 350, 3) tf.Tensor(1, shape=(), dtype=int64)
    tf.Tensor(1, shape=(), dtype=int64)
:::

::: {.output .display_data}
![](5573d5d782e09b9d8bc97dd8e1cc86dc7bd7d215.png)
:::
::::::::::::

::: {.cell .code id="msAbOlowHyAH"}
``` python
IMG_SIZE = 150
```
:::

::: {.cell .code id="eNrYnCCGG_HV"}
``` python
def resize_normalize(image, label):
  # resize the image
  image = tf.image.resize(image, [IMG_SIZE, IMG_SIZE])
  # normalize the image
  image = (image / 255.0)
  return image, label
```
:::

::: {.cell .code id="okxY5EHVSUdw"}
``` python
dataset = dataset.map(resize_normalize)
```
:::

::::: {.cell .code colab="{\"base_uri\":\"https://localhost:8080/\",\"height\":452}" id="cxfGdDmESfrK" outputId="c7fe360b-f95c-4cf6-fce9-e1e669a83132"}
``` python
for image, label in dataset.take(1):
  plt.imshow(image)
  print(image.shape, label)
```

::: {.output .stream .stdout}
    (150, 150, 3) tf.Tensor(1, shape=(), dtype=int64)
:::

::: {.output .display_data}
![](0828efea1b57035eb1973b880d24e1bdd275739e.png)
:::
:::::

::: {.cell .code id="yOCSjDN3TTDk"}
``` python
image, label = next(iter(dataset))
```
:::

::::: {.cell .code colab="{\"base_uri\":\"https://localhost:8080/\",\"height\":308}" id="DC6PIHvjTfUM" outputId="056ab723-c2bb-46b1-eeb2-f9d981e58673"}
``` python
plt.subplot(1, 2, 1)
plt.imshow(image)
plt.subplot(1, 2, 2)
plt.imshow(tf.image.flip_left_right(image))
```

::: {.output .execute_result execution_count="75"}
    <matplotlib.image.AxesImage at 0x7a7bbe4a0340>
:::

::: {.output .display_data}
![](b85c706ed51d05119ebd20a854f8343542eebeab.png)
:::
:::::

::::: {.cell .code colab="{\"base_uri\":\"https://localhost:8080/\",\"height\":308}" id="6nL923T9UtkY" outputId="55d72fa2-c5cc-4c2d-d677-eef1527e0cc4"}
``` python
plt.subplot(1, 2, 1)
plt.imshow(image)
plt.subplot(1, 2, 2)
plt.imshow(tf.image.rot90(image))
```

::: {.output .execute_result execution_count="89"}
    <matplotlib.image.AxesImage at 0x7a7bbd7d7760>
:::

::: {.output .display_data}
![](2381956d77e2853707421aa6894402056b3a0177.png)
:::
:::::

:::::: {.cell .code colab="{\"base_uri\":\"https://localhost:8080/\",\"height\":326}" id="GB_biegiU5xU" outputId="0a4db314-e8d0-476e-ae51-2aca13e68519"}
``` python
plt.subplot(1, 2, 1)
plt.imshow(image)
plt.subplot(1, 2, 2)
plt.imshow(tf.image.random_brightness(image, max_delta=0.5))
```

::: {.output .stream .stderr}
    WARNING:matplotlib.image:Clipping input data to the valid range for imshow with RGB data ([0..1] for floats or [0..255] for integers).
:::

::: {.output .execute_result execution_count="92"}
    <matplotlib.image.AxesImage at 0x7a7bbd258070>
:::

::: {.output .display_data}
![](b0902ccaf2999588c03971c5ef90ff4841cd8dfe.png)
:::
::::::

::: {.cell .code id="XKsHgretHP1Q"}
``` python
def augment(image, label):
  image = tf.image.random_flip_left_right(image)
  image = tf.image.rot90(image)
  #image = tf.image.random_contrast(image, lower=0.2, upper=0.8)
  image = tf.image.random_brightness(image, max_delta=0.5)
  return image, label
```
:::

::::: {.cell .code colab="{\"base_uri\":\"https://localhost:8080/\",\"height\":384}" id="uyKWwK1bQ8mt" outputId="8b0b1576-4d81-4817-b223-35bef90c568b"}
``` python
plt.subplot(1, 2, 1)
plt.imshow(image)
plt.subplot(1, 2, 2)
plt.imshow(augment(image, label)[0])
```

::: {.output .execute_result execution_count="100"}
    <matplotlib.image.AxesImage at 0x7a7bbd5a2890>
:::

::: {.output .display_data}
![](840fd68ad3702fbfcdca7d6af186ff97eb67e003.png)
:::
:::::

::: {.cell .code id="npsz7TdMPZFi"}
``` python
```
:::
