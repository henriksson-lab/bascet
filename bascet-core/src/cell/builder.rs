pub trait Builder: Sized {
    type Product: super::Cell;
    fn produce(self) -> Self::Product;

    fn managed_ref(self, _: <Self::Product as super::marker::UseManagedRef>::Ref) -> Self
    where
        Self::Product: super::marker::UseManagedRef,
    {
        #[cfg(debug_assertions)]
        {
            // TODO: add debug logging
        }
        self
    }

    fn id(self, _: <Self::Product as super::marker::ProvideID>::Type) -> Self
    where
        Self::Product: super::marker::ProvideID,
    {
        #[cfg(debug_assertions)]
        {
            // TODO: add debug logging
        }
        self
    }

    fn read_pair(self, _: <Self::Product as super::marker::ProvideReadPair>::Type) -> Self
    where
        Self::Product: super::marker::ProvideReadPair,
    {
        #[cfg(debug_assertions)]
        {
            // TODO: add debug logging
        }
        self
    }

    fn read(self, _: <Self::Product as super::marker::ProvideRead>::Type) -> Self
    where
        Self::Product: super::marker::ProvideRead,
    {
        #[cfg(debug_assertions)]
        {
            // TODO: add debug logging
        }
        self
    }

    fn quality_pair(self, _: <Self::Product as super::marker::ProvideQualityPair>::Type) -> Self
    where
        Self::Product: super::marker::ProvideQualityPair,
    {
        #[cfg(debug_assertions)]
        {
            // TODO: add debug logging
        }
        self
    }

    fn quality(self, _: <Self::Product as super::marker::ProvideQuality>::Type) -> Self
    where
        Self::Product: super::marker::ProvideQuality,
    {
        #[cfg(debug_assertions)]
        {
            // TODO: add debug logging
        }
        self
    }

    fn umi(self, _: <Self::Product as super::marker::ProvideUMI>::Type) -> Self
    where
        Self::Product: super::marker::ProvideUMI,
    {
        #[cfg(debug_assertions)]
        {
            // TODO: add debug logging
        }
        self
    }

    fn metadata(self, _: <Self::Product as super::marker::ProvideMetadata>::Type) -> Self
    where
        Self::Product: super::marker::ProvideMetadata,
    {
        #[cfg(debug_assertions)]
        {
            // TODO: add debug logging
        }
        self
    }

}