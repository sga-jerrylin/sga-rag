import { useContext, useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import { NavLink, Outlet, useNavigate } from 'react-router';

import { useMutation, useQuery } from '@tanstack/react-query';

import {
  LucideActivity,
  LucideBrain,
  LucideLayoutDashboard,
  LucideListTree,
  LucideMonitor,
  LucideServerCrash,
  LucideSettings,
  LucideSquareUserRound,
  LucideUserCog,
  LucideUserStar,
  LucideZap,
} from 'lucide-react';

import { Button } from '@/components/ui/button';
import { useBranding } from '@/hooks/use-branding';
import { cn } from '@/lib/utils';
import { Routes } from '@/routes';
import { getSystemVersion, logout } from '@/services/admin-service';

import authorizationUtil from '@/utils/authorization-util';

import ThemeSwitch from '../../../components/theme-switch';
import { IS_ENTERPRISE } from '../utils';
import { CurrentUserInfoContext } from './root-layout';

const AdminNavigationLayout = () => {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const [, setCurrentUserInfo] = useContext(CurrentUserInfoContext);
  const { branding } = useBranding();

  const { data: version } = useQuery({
    queryKey: ['admin/version'],
    queryFn: async () => (await getSystemVersion())?.data?.data?.version,
  });

  const navItems = useMemo(
    () => [
      {
        path: Routes.AdminDashboard,
        name: t('admin.dashboard', 'KM 仪表盘'),
        icon: <LucideLayoutDashboard className="size-[1em]" />,
      },
      {
        path: Routes.AdminPipeline,
        name: t('admin.pipeline', '流水线监控'),
        icon: <LucideListTree className="size-[1em]" />,
      },
      {
        path: Routes.AdminMemories,
        name: t('admin.memories', '记忆管理'),
        icon: <LucideBrain className="size-[1em]" />,
      },
      {
        path: Routes.AdminObservability,
        name: t('admin.observability', '可观测性'),
        icon: <LucideActivity className="size-[1em]" />,
      },
      {
        path: Routes.AdminServices,
        name: t('admin.serviceStatus'),
        icon: <LucideServerCrash className="size-[1em]" />,
      },
      {
        path: Routes.AdminUserManagement,
        name: t('admin.userManagement'),
        icon: <LucideUserCog className="size-[1em]" />,
      },
      {
        path: Routes.AdminSandboxSettings,
        name: t('admin.sandboxSettings'),
        icon: <LucideZap className="size-[1em]" />,
      },
      ...(IS_ENTERPRISE
        ? [
            {
              path: Routes.AdminWhitelist,
              name: t('admin.registrationWhitelist'),
              icon: <LucideUserStar className="size-[1em]" />,
            },
            {
              path: Routes.AdminRoles,
              name: t('admin.roles'),
              icon: <LucideSquareUserRound className="size-[1em]" />,
            },
            {
              path: Routes.AdminMonitoring,
              name: t('admin.monitoring'),
              icon: <LucideMonitor className="size-[1em]" />,
            },
          ]
        : []),
      {
        path: Routes.AdminSettings,
        name: t('admin.systemSettings'),
        icon: <LucideSettings className="size-[1em]" />,
      },
    ],
    [t],
  );

  const logoutMutation = useMutation({
    mutationKey: ['adminLogout'],
    mutationFn: async () => {
      await logout();
      authorizationUtil.removeAll();
      navigate(Routes.Admin);
      setCurrentUserInfo({
        userInfo: null,
        source: null,
      });
    },
    retry: false,
  });

  return (
    <main className="w-screen h-screen flex flex-row px-6 pt-12 pb-6 dark:*:focus-visible:ring-white">
      <aside className="w-72 mr-6 flex flex-col gap-6">
        <div className="flex items-center mb-6">
          <img
            className="h-14 w-auto max-w-[200px] mr-3 object-contain"
            src={branding.loginLogoUrl}
            alt="logo"
          />
          <span className="text-xl font-bold">{branding.productName}</span>
        </div>

        <nav>
          <ul className="space-y-4">
            {navItems.map((it) => (
              <li key={it.path}>
                <NavLink
                  to={it.path}
                  className={({ isActive }) =>
                    cn(
                      'px-4 py-3 rounded-lg',
                      'text-base w-full flex items-center justify-start text-text-secondary',
                      'hover:bg-bg-card focus:bg-bg-card focus-visible:bg-bg-card',
                      'hover:text-text-primary focus:text-text-primary focus-visible:text-text-primary',
                      'active:text-text-primary',
                      'transition-colors',
                      {
                        'bg-bg-card text-text-primary': isActive,
                      },
                    )
                  }
                >
                  {it.icon}
                  <span className="ml-3">{it.name}</span>
                </NavLink>
              </li>
            ))}
          </ul>
        </nav>

        <div className="mt-auto space-y-4">
          <div className="flex justify-between items-center">
            <span className="leading-none text-xs text-accent-primary">
              {version}
            </span>

            <ThemeSwitch />
          </div>

          <Button
            size="lg"
            variant="transparent"
            block
            onClick={() => logoutMutation.mutate()}
          >
            {t('header.logout')}
          </Button>
        </div>
      </aside>

      <section className="flex-1 h-full">
        <Outlet />
      </section>
    </main>
  );
};

export default AdminNavigationLayout;
