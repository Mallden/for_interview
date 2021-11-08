# coding=utf-8
import math
import threading
import xml.etree.ElementTree as ElementTree
from collections import defaultdict

from progress.bar import Bar

from apps.models.models import Product, ProductModel, Characteristic, ProductModelImage, ProductCharacteristic
from .app import celery


class GenPriceList:

    def __init__(self, price_list_id, price_list_path, company_id):
        self.company_id = company_id
        self.price_list_id = price_list_id
        self.price_list_path = price_list_path
        self.tree = ElementTree.parse(self.price_list_path)
        self.root = self.tree.getroot()
        self.chunk_count = 5000
        self.threads = list()
        self.all_product = Product.objects.filter(company_id=self.company_id, yml_pricelist_id=self.price_list_id)
        self.all_create_model = ProductModel.objects.filter(title__in=self.all_product.values_list('title', flat=True))

    def _iter_chunks(self, offers):
        bar = Bar('Iter offers', max=len(offers))
        list_new_model = list()
        origin_products = list()
        chars_dict = defaultdict(list)
        for offer in offers:
            bar.next()
            name = offer.find('name').text
            if not name or self.all_create_model.filter(title=name).exists():
                continue

            try:
                product = self.all_product.get(title=name)
                origin_products.append(product)
            except Product.DoesNotExist:
                continue

            chars = offer.findall('param')
            if chars:
                for char in chars:
                    chars_create, _c = Characteristic.objects.get_or_create(
                        group_name='{} {}'.format(char.attrib['name'], char.attrib['unit']),
                        value=char.text
                    )

                    if _c:
                        chars_create.save()

                    chars_dict[product.id].append(chars_create)

            list_new_model.append(
                ProductModel(
                    title=product.title,
                    origin_product=product,
                    section_id=product.section_id,
                    origin_company_id=product.company_id,
                    description=product.description,
                    description_tag=product.description_tag,
                    brand_id=product.brand_id,
                    active=True,
                )
            )
        bar.finish()
        ProductModel.objects.bulk_create(list_new_model)
        product_models = ProductModel.objects.filter(origin_product__in=origin_products)
        image_models = [
            ProductModelImage(
                model=m,
                image=m.origin_product.images.first().image,
                cover=m.origin_product.images.first().cover
            ) for m in product_models if m.origin_product.images.exists()
        ]
        ProductModelImage.objects.bulk_create(image_models)
        bulk_chars = list()
        for prod, chars in chars_dict:
            bulk_chars.extend([
                ProductCharacteristic(
                    product_model=product_models.get(origin_product=prod),
                    characteristic=char
                ) for char in chars
            ])

        ProductCharacteristic.objects.bulk_create(bulk_chars)

    def start(self):
        shop = self.root.find('shop')
        offers = shop.find('offers')
        if not offers:
            return 'Not offers in price list'

        count_offers = len(offers)
        chunks = int(math.ceil(len(count_offers) / float(self.chunk_count)))
        print 'Chunk count {}'.format(str(chunks))

        for j in range(chunks):
            thread = threading.Thread(
                target=self._iter_chunks,
                args=[offers[j * self.chunk_count:((j + 1) * self.chunk_count) + 1]]
            )
            print 'Chunk {}'.format(str(j + 1))
            thread.start()
            self.threads.append(thread)
        for thread in self.threads:
            thread.join()

        return 'Success!'


@celery.task
def create_model_from_yml(price_list_id, price_list_path, company_id):
    gen = GenPriceList(price_list_id, price_list_path, company_id)
    return gen.start()
